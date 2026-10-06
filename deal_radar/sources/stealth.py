"""Browser hardening and human pacing shared by Playwright-driven sources.

Why this module exists
----------------------
Marketplaces without a public API (Facebook Marketplace first of all) are read with a
real Chromium driven by Playwright, logged in with the operator's *own* account. An
automated Chromium differs from the browser a person uses in a handful of well-known,
cheap-to-check places (``navigator.webdriver``, an empty plugin list in the headless
shell, a ``HeadlessChrome`` User-Agent, a software WebGL renderer, inconsistent
notification permissions...). Bot-management scripts read those surfaces and
correlate them with network-level signals.

The guiding principle here is **consistency, not randomisation**:

* every observable attribute should tell the *same* story — the egress IP's
  geolocation ≈ the emulated ``geolocation`` ≈ ``timezone_id`` ≈ ``locale`` ≈
  ``navigator.languages`` ≈ the ``Accept-Language`` header ≈ the UA's platform ≈
  ``navigator.platform`` ≈ the WebGL renderer. A browser that claims New York while
  its clock runs on UTC and its IP sits in a Frankfurt datacenter is far more
  suspicious than one with a perfectly ordinary, *stable* fingerprint;
* values are only rewritten where automation makes them *implausible* (no plugins,
  software rasteriser, ``webdriver === true``); real values are left untouched;
* nothing is randomised per session or per poll. A logged-in session is a long-lived
  relationship with the site: its cookies are bound to the device fingerprint they
  were issued to, so rotating the User-Agent or screen size between polls turns one
  "returning visitor" into a stream of impossible device changes on the same account
  and invalidates the trust the session has accumulated.

What this module deliberately does **not** do: solve CAPTCHAs, automate logins,
create or rotate accounts, or hide from explicit blocks. When a site shows a
checkpoint the caller is expected to stop and tell the operator (see
:class:`~deal_radar.sources.base.SourceBlocked`).

Pacing helpers (:func:`human_pause`, :func:`human_scroll`) keep request cadence close
to a person browsing: right-skewed think times and wheel scrolling in uneven flicks
instead of instant ``window.scrollTo`` jumps.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import platform as _platform
import random
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote, urlsplit

from deal_radar.core.logs import get_logger

if TYPE_CHECKING:  # pragma: no cover
    from playwright.async_api import Browser, BrowserContext, Page, Playwright

    from deal_radar.config_schema import BrowserSection, GeoPin

try:
    from playwright.async_api import Error as PlaywrightError
except ImportError:  # pragma: no cover - playwright is a declared dependency

    class PlaywrightError(Exception):  # type: ignore[no-redef]
        """Stand-in so this module stays importable without Playwright."""


log = get_logger("sources.stealth")

# --------------------------------------------------------------------------- init script

#: Single, idempotent init script (``context.add_init_script``). It runs before any page
#: script in every frame. Each patch first checks whether the surface is already
#: consistent and only then rewrites it, so evaluating the script twice (or once in a
#: frame and once more through the ``contentWindow`` hook) is harmless. Patched
#: functions are object-literal methods/accessors (no ``prototype`` property, correct
#: ``name``) registered with a ``Function.prototype.toString`` proxy so their source
#: reads ``function get languages() { [native code] }`` like the built-ins they replace.
STEALTH_INIT_SCRIPT = r"""
(() => {
  'use strict';
  if (typeof window === 'undefined') return;

  const nativeSource = new WeakMap();   // patched function -> source text it reports
  const fakes = new WeakMap();          // synthetic platform object -> backing data
  const patchedRealms = new WeakSet();  // windows already handled by this evaluation
  const SOFTWARE_GL = /swiftshader|llvmpipe|softpipe|software|mesa offscreen/i;

  const nativeText = (name) => `function ${name}() { [native code] }`;
  const mask = (fn, name) => { nativeSource.set(fn, nativeText(name)); return fn; };

  // Re-capture an error's stack below `outer` (the patched function the page called) so
  // our helper frames never show up; native frames on top (e.g. "at Number.toString
  // (<anonymous>)") are kept, exactly like an error thrown by the built-in.
  function cleanStack(err, outer) {
    try {
      if (!err || typeof err !== 'object' || typeof err.stack !== 'string' || !Error.captureStackTrace) return err;
      const lines = err.stack.split('\n');
      const head = [lines[0]];
      for (const line of lines.slice(1)) {
        if (/^\s+at .*\(<anonymous>\)$/.test(line) || /^\s+at .*\(native\)$/.test(line)) head.push(line);
        else break;
      }
      const holder = {};
      Error.captureStackTrace(holder, outer);
      err.stack = head.concat(String(holder.stack).split('\n').slice(1)).join('\n');
    } catch (_) { /* frozen error objects */ }
    return err;
  }

  function callNative(fn, self, args, outer) {
    try { return Reflect.apply(fn, self, args); } catch (err) { throw cleanStack(err, outer); }
  }

  // impl(self, outer): `outer` is the getter itself, for cleanStack().
  function makeGetter(prop, impl) {
    let getter = null;
    const holder = { get [prop]() { return impl(this, getter); } };
    getter = Object.getOwnPropertyDescriptor(holder, prop).get;
    return mask(getter, `get ${prop}`);
  }

  // impl(self, args, outer)
  function makeMethod(name, length, impl) {
    let method = null;
    const holder = { [name](...args) { return impl(this, args, method); } };
    method = holder[name];
    Object.defineProperty(method, 'length', { value: length, configurable: true });
    return mask(method, name);
  }

  function defineGetter(target, prop, getter) {
    const desc = Object.getOwnPropertyDescriptor(target, prop);
    if (desc && !desc.configurable) return false;
    Object.defineProperty(target, prop, {
      get: getter,
      set: desc ? desc.set : undefined,
      enumerable: desc ? desc.enumerable : true,
      configurable: true,
    });
    return true;
  }

  // Replace a getter's value while keeping the native brand check: called on anything
  // that is not a `brand` instance it defers to the original (-> "Illegal invocation").
  function overrideGetter(target, prop, brand, valueOf) {
    const desc = Object.getOwnPropertyDescriptor(target, prop);
    const nativeGet = desc && desc.get;
    return defineGetter(target, prop, makeGetter(prop, (self, outer) => {
      if (nativeGet && brand && !(self instanceof brand)) return callNative(nativeGet, self, [], outer);
      return valueOf(self);
    }));
  }

  function replaceMethod(target, name, fn) {
    const desc = Object.getOwnPropertyDescriptor(target, name);
    if (desc && !desc.configurable) return false;
    Object.defineProperty(target, name, {
      value: fn,
      writable: desc ? desc.writable : true,
      enumerable: desc ? desc.enumerable : true,
      configurable: true,
    });
    return true;
  }

  // Accessors that answer from `fakes` for synthetic objects and defer to the native
  // implementation (brand checks included) for real ones.
  function patchFakeAccessors(proto, props) {
    for (const prop of props) {
      const desc = Object.getOwnPropertyDescriptor(proto, prop);
      if (!desc || !desc.get || nativeSource.has(desc.get)) continue;
      const nativeGet = desc.get;
      defineGetter(proto, prop, makeGetter(prop, (self, outer) => {
        const data = fakes.get(self);
        return data ? data[prop] : callNative(nativeGet, self, [], outer);
      }));
    }
  }

  function patchFakeCollection(proto, { named = true, refresh = false } = {}) {
    const lengthDesc = Object.getOwnPropertyDescriptor(proto, 'length');
    if (lengthDesc && lengthDesc.get && !nativeSource.has(lengthDesc.get)) {
      const nativeLength = lengthDesc.get;
      defineGetter(proto, 'length', makeGetter('length', (self, outer) => {
        const data = fakes.get(self);
        return data ? data.items.length : callNative(nativeLength, self, [], outer);
      }));
    }
    const nativeItem = proto.item;
    if (typeof nativeItem === 'function' && !nativeSource.has(nativeItem)) {
      replaceMethod(proto, 'item', makeMethod('item', 1, (self, args, outer) => {
        const data = fakes.get(self);
        if (!data) return callNative(nativeItem, self, args, outer);
        const value = data.items[Number(args[0]) >>> 0];
        return value === undefined ? null : value;
      }));
    }
    const nativeNamed = proto.namedItem;
    if (named && typeof nativeNamed === 'function' && !nativeSource.has(nativeNamed)) {
      replaceMethod(proto, 'namedItem', makeMethod('namedItem', 1, (self, args, outer) => {
        const data = fakes.get(self);
        if (!data) return callNative(nativeNamed, self, args, outer);
        const value = data.named.get(String(args[0]));
        return value === undefined ? null : value;
      }));
    }
    const nativeRefresh = proto.refresh;
    if (refresh && typeof nativeRefresh === 'function' && !nativeSource.has(nativeRefresh)) {
      replaceMethod(proto, 'refresh', makeMethod('refresh', 0, (self, args, outer) => {
        if (!fakes.get(self)) return callNative(nativeRefresh, self, args, outer);
        return undefined;
      }));
    }
  }

  function fillIndexed(obj, items, keyOf) {
    const named = new Map();
    items.forEach((item, index) => {
      Object.defineProperty(obj, index, { value: item, enumerable: true, configurable: true });
      named.set(keyOf(item), item);
    });
    for (const [key, item] of named) {
      if (!(key in obj)) Object.defineProperty(obj, key, { value: item, enumerable: false, configurable: true });
    }
    return named;
  }

  // ------------------------------------------------------------------ patches

  function patchToString(win) {
    const proto = win.Function.prototype;
    const current = proto.toString;
    if (nativeSource.has(current)) return;
    const handler = {
      apply(target, thisArg, args) {
        if (nativeSource.has(thisArg)) return nativeSource.get(thisArg);
        return callNative(target, thisArg, args, handler.apply);
      },
    };
    const proxy = new Proxy(current, handler);
    nativeSource.set(proxy, nativeText('toString'));
    replaceMethod(proto, 'toString', proxy);
  }

  function patchNavigator(win) {
    const nav = win.navigator;
    const proto = win.Navigator && win.Navigator.prototype;
    if (!nav || !proto) return;

    // Modern Chrome reports `false` (property present). --disable-blink-features=
    // AutomationControlled already does that natively; this is the fallback.
    const brand = win.Navigator;
    if (nav.webdriver === true) {
      if (!overrideGetter(proto, 'webdriver', brand, () => false)) {
        try { delete proto.webdriver; } catch (_) { /* non-configurable */ }
      }
    }

    // Playwright's locale emulation yields ['en-US']; Chrome sends ['en-US', 'en'],
    // matching the Accept-Language header the context sets.
    const lang = nav.language;
    const langs = nav.languages;
    if (typeof lang === 'string' && lang.includes('-') && (!langs || langs.length < 2)) {
      const frozen = win.Object.freeze(win.Array.of(lang, lang.split('-')[0]));
      overrideGetter(proto, 'languages', brand, () => frozen);
    }

    if (!(nav.hardwareConcurrency >= 2)) {
      overrideGetter(proto, 'hardwareConcurrency', brand, () => 4);
    }
    // deviceMemory only exists in secure contexts; leave it absent elsewhere.
    if ('deviceMemory' in proto && !(nav.deviceMemory >= 2)) {
      overrideGetter(proto, 'deviceMemory', brand, () => 8);
    }
  }

  function patchPlugins(win) {
    const nav = win.navigator;
    const proto = win.Navigator && win.Navigator.prototype;
    if (!proto || !win.PluginArray || !win.Plugin || !win.MimeType || !win.MimeTypeArray) return;
    let count = 0;
    try { count = nav.plugins.length; } catch (_) { return; }
    if (count > 0) return;

    const make = (p, data) => { const obj = Object.create(p); fakes.set(obj, data); return obj; };
    const mimeSpecs = [
      ['application/pdf', 'pdf', 'Portable Document Format'],
      ['text/pdf', 'pdf', 'Portable Document Format'],
    ];
    // The fixed list every desktop Chrome has reported since the PDF viewer refactor.
    const pluginNames = ['PDF Viewer', 'Chrome PDF Viewer', 'Chromium PDF Viewer',
      'Microsoft Edge PDF Viewer', 'WebKit built-in PDF'];
    const typeOf = (m) => fakes.get(m).type;

    const plugins = pluginNames.map((name) => make(win.Plugin.prototype, {
      name, filename: 'internal-pdf-viewer', description: 'Portable Document Format', items: [], named: new Map(),
    }));
    for (const plugin of plugins) {
      const data = fakes.get(plugin);
      data.items = mimeSpecs.map(([type, suffixes, description]) =>
        make(win.MimeType.prototype, { type, suffixes, description, enabledPlugin: plugin }));
      data.named = fillIndexed(plugin, data.items, typeOf);
    }
    const mimeTypes = mimeSpecs.map(([type, suffixes, description]) =>
      make(win.MimeType.prototype, { type, suffixes, description, enabledPlugin: plugins[0] }));
    const pluginArray = make(win.PluginArray.prototype, { items: plugins, named: new Map() });
    fakes.get(pluginArray).named = fillIndexed(pluginArray, plugins, (p) => fakes.get(p).name);
    const mimeArray = make(win.MimeTypeArray.prototype, { items: mimeTypes, named: new Map() });
    fakes.get(mimeArray).named = fillIndexed(mimeArray, mimeTypes, typeOf);

    patchFakeAccessors(win.Plugin.prototype, ['name', 'filename', 'description']);
    patchFakeAccessors(win.MimeType.prototype, ['type', 'suffixes', 'description', 'enabledPlugin']);
    patchFakeCollection(win.Plugin.prototype);
    patchFakeCollection(win.PluginArray.prototype, { refresh: true });
    patchFakeCollection(win.MimeTypeArray.prototype);

    overrideGetter(proto, 'plugins', win.Navigator, () => pluginArray);
    overrideGetter(proto, 'mimeTypes', win.Navigator, () => mimeArray);
    if ('pdfViewerEnabled' in proto && nav.pdfViewerEnabled === false) {
      overrideGetter(proto, 'pdfViewerEnabled', win.Navigator, () => true);
    }
  }

  function patchChrome(win) {
    let chrome = win.chrome;
    if (!chrome) {
      chrome = {};
      Object.defineProperty(win, 'chrome', { value: chrome, writable: true, enumerable: true, configurable: false });
    }
    if (!('app' in chrome)) {
      chrome.app = {
        isInstalled: false,
        InstallState: { DISABLED: 'disabled', INSTALLED: 'installed', NOT_INSTALLED: 'not_installed' },
        RunningState: { CANNOT_RUN: 'cannot_run', READY_TO_RUN: 'ready_to_run', RUNNING: 'running' },
        getDetails: makeMethod('getDetails', 0, () => null),
        getIsInstalled: makeMethod('getIsInstalled', 0, () => false),
        runningState: makeMethod('runningState', 0, () => 'cannot_run'),
      };
    }
    if (!('csi' in chrome)) {
      chrome.csi = makeMethod('csi', 0, () => {
        const t = win.performance.timing;
        return { onloadT: t.domContentLoadedEventEnd, startE: t.navigationStart, pageT: win.performance.now(), tran: 15 };
      });
    }
    if (!('loadTimes' in chrome)) {
      chrome.loadTimes = makeMethod('loadTimes', 0, () => {
        const t = win.performance.timing;
        const entry = (win.performance.getEntriesByType('navigation') || [])[0];
        const proto = (entry && entry.nextHopProtocol) || 'http/1.1';
        const spdy = proto === 'h2' || proto === 'h3';
        return {
          requestTime: t.navigationStart / 1000,
          startLoadTime: t.navigationStart / 1000,
          commitLoadTime: t.responseStart / 1000,
          finishDocumentLoadTime: t.domContentLoadedEventEnd / 1000,
          finishLoadTime: t.loadEventEnd / 1000,
          firstPaintTime: (t.domInteractive || t.responseEnd) / 1000,
          firstPaintAfterLoadTime: 0,
          navigationType: 'Other',
          wasFetchedViaSpdy: spdy,
          wasNpnNegotiated: spdy,
          npnNegotiatedProtocol: spdy ? proto : 'unknown',
          wasAlternateProtocolAvailable: false,
          connectionInfo: proto,
        };
      });
    }
    // Desktop Chrome exposes chrome.runtime to pages on secure origins only.
    if (win.isSecureContext && !('runtime' in chrome)) {
      const noId = (method, signature) => makeMethod(method, 0, (self, args, outer) => {
        throw cleanStack(new win.TypeError(`Error in invocation of runtime.${method}(${signature}): ` +
          `chrome.runtime.${method}() called from a webpage must specify an Extension ID (string) ` +
          'for its first argument.'), outer);
      });
      chrome.runtime = {
        OnInstalledReason: {
          CHROME_UPDATE: 'chrome_update', INSTALL: 'install', SHARED_MODULE_UPDATE: 'shared_module_update', UPDATE: 'update',
        },
        OnRestartRequiredReason: { APP_UPDATE: 'app_update', OS_UPDATE: 'os_update', PERIODIC: 'periodic' },
        PlatformArch: { ARM: 'arm', ARM64: 'arm64', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' },
        PlatformNaclArch: { ARM: 'arm', MIPS: 'mips', MIPS64: 'mips64', X86_32: 'x86-32', X86_64: 'x86-64' },
        PlatformOs: {
          ANDROID: 'android', CROS: 'cros', FUCHSIA: 'fuchsia', LINUX: 'linux', MAC: 'mac', OPENBSD: 'openbsd', WIN: 'win',
        },
        RequestUpdateCheckStatus: { NO_UPDATE: 'no_update', THROTTLED: 'throttled', UPDATE_AVAILABLE: 'update_available' },
        id: undefined,
        connect: noId('connect', 'optional string extensionId, optional object connectInfo'),
        sendMessage: noId('sendMessage',
          'optional string extensionId, any message, optional object options, optional function callback'),
      };
    }
  }

  // Headless Chromium answers permissions.query({name: 'notifications'}) with a state
  // that contradicts Notification.permission; real Chrome keeps them in lock-step.
  function patchPermissions(win) {
    const proto = win.Permissions && win.Permissions.prototype;
    const statusProto = win.PermissionStatus && win.PermissionStatus.prototype;
    if (!proto || !statusProto || typeof proto.query !== 'function' || !win.Notification) return;
    const nativeQuery = proto.query;
    if (nativeSource.has(nativeQuery)) return;
    patchFakeAccessors(statusProto, ['state', 'name', 'onchange']);
    replaceMethod(proto, 'query', makeMethod('query', 1, (self, args, outer) => {
      const result = callNative(nativeQuery, self, args, outer);
      const params = args[0];
      if (!params || params.name !== 'notifications') return result;
      return result.then((status) => {
        const current = win.Notification.permission;
        const expected = current === 'default' ? 'prompt' : current;
        if (!status || status.state === expected) return status;
        const fake = Object.create(statusProto);
        fakes.set(fake, { state: expected, name: 'notifications', onchange: null });
        return fake;
      });
    }));
  }

  // SwiftShader/llvmpipe renderers only exist on GPU-less servers and headless runs.
  function patchWebGl(win) {
    const platform = String(win.navigator.platform || '').toLowerCase();
    let vendor = 'Google Inc. (Intel)';
    let renderer = 'ANGLE (Intel, Mesa Intel(R) UHD Graphics 630 (CFL GT2), OpenGL 4.6)';
    if (platform.startsWith('win')) {
      vendor = 'Google Inc. (NVIDIA)';
      renderer = 'ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 SUPER (0x000021C4) Direct3D11 vs_5_0 ps_5_0, D3D11)';
    } else if (platform.startsWith('mac')) {
      vendor = 'Google Inc. (Intel Inc.)';
      renderer = 'ANGLE (Intel Inc., Intel(R) UHD Graphics 630, OpenGL 4.1)';
    }
    const UNMASKED_VENDOR = 0x9245;
    const UNMASKED_RENDERER = 0x9246;
    for (const ctorName of ['WebGLRenderingContext', 'WebGL2RenderingContext']) {
      const ctor = win[ctorName];
      if (!ctor) continue;
      const proto = ctor.prototype;
      const nativeGet = proto.getParameter;
      if (typeof nativeGet !== 'function' || nativeSource.has(nativeGet)) continue;
      replaceMethod(proto, 'getParameter', makeMethod('getParameter', 1, (self, args, outer) => {
        const value = callNative(nativeGet, self, args, outer);
        const param = args[0];
        if (param !== UNMASKED_VENDOR && param !== UNMASKED_RENDERER) return value;
        const actual = param === UNMASKED_RENDERER ? value : callNative(nativeGet, self, [UNMASKED_RENDERER], outer);
        if (typeof actual !== 'string' || !SOFTWARE_GL.test(actual)) return value;
        return param === UNMASKED_VENDOR ? vendor : renderer;
      }));
    }
  }

  // Headless windows have no browser chrome: outer size == inner size.
  function patchOuterSize(win) {
    if (win.top !== win) return;
    if (win.outerWidth !== 0 && win.outerHeight !== 0 &&
        !(win.outerWidth === win.innerWidth && win.outerHeight === win.innerHeight)) return;
    defineGetter(win, 'outerWidth', makeGetter('outerWidth', () => win.innerWidth));
    defineGetter(win, 'outerHeight', makeGetter('outerHeight', () => win.innerHeight + 85));
  }

  // Same-origin iframes created by script can be inspected before any init script ran
  // in them; patch their realm on first access so they match the top document.
  function patchIframes(win) {
    const proto = win.HTMLIFrameElement && win.HTMLIFrameElement.prototype;
    if (!proto) return;
    const desc = Object.getOwnPropertyDescriptor(proto, 'contentWindow');
    if (!desc || !desc.get || nativeSource.has(desc.get)) return;
    const nativeGet = desc.get;
    defineGetter(proto, 'contentWindow', makeGetter('contentWindow', (self, outer) => {
      const child = callNative(nativeGet, self, [], outer);
      if (child) {
        try { applyAll(child); } catch (_) { /* cross-origin frame */ }
      }
      return child;
    }));
  }

  function applyAll(win) {
    if (patchedRealms.has(win)) return;
    if (!win.Function) return;  // throws SecurityError for cross-origin windows
    patchedRealms.add(win);
    const steps = [patchToString, patchNavigator, patchPlugins, patchChrome, patchPermissions,
      patchWebGl, patchOuterSize, patchIframes];
    for (const step of steps) {
      try { step(win); } catch (_) { /* never break the page */ }
    }
  }

  applyAll(window);
})();
"""

# --------------------------------------------------------------------------- identity

#: Chromium switches applied to every launch.
#: ``AutomationControlled`` is what flips ``navigator.webdriver`` to true; the rest
#: suppress first-run UI and the Linux keyring prompt that a person would never see twice.
BROWSER_ARGS: tuple[str, ...] = (
    "--disable-blink-features=AutomationControlled",
    "--no-first-run",
    "--no-default-browser-check",
    "--password-store=basic",
)
#: Playwright adds ``--enable-automation`` (infobar + webdriver flag); drop it.
IGNORED_DEFAULT_ARGS: tuple[str, ...] = ("--enable-automation",)

#: Desktop screen sizes that are common in the wild, smallest first.
COMMON_SCREENS: tuple[tuple[int, int], ...] = (
    (1366, 768),
    (1440, 900),
    (1536, 864),
    (1600, 900),
    (1680, 1050),
    (1920, 1080),
    (1920, 1200),
    (2560, 1440),
    (3840, 2160),
)
#: Vertical pixels taken by tab strip, toolbar and OS panel above/below the viewport.
BROWSER_UI_HEIGHT = 120
#: Reported accuracy (metres) of the emulated geolocation — Wi-Fi positioning grade.
GEO_ACCURACY_METERS = 50.0

# UA reduction (Chrome 110+) freezes the platform token per OS family.
_UA_PLATFORM = {
    "Linux": "X11; Linux x86_64",
    "Darwin": "Macintosh; Intel Mac OS X 10_15_7",
    "Windows": "Windows NT 10.0; Win64; x64",
}


def normalize_locale(locale: str) -> str:
    """BCP-47 form of a locale (``en_US`` -> ``en-US``); empty -> ``en-US``."""
    return locale.replace("_", "-").strip() or "en-US"


def accept_languages(locale: str) -> list[str]:
    """Language list a desktop Chrome derives from its UI locale: region tag + base."""
    tag = normalize_locale(locale)
    base = tag.split("-", 1)[0]
    return [tag, base] if base != tag else [tag]


def accept_language(locale: str) -> str:
    """``Accept-Language`` value Chrome sends for ``locale`` (``en-US,en;q=0.9``)."""
    langs = accept_languages(locale)
    return ",".join([langs[0], *(f"{lang};q=0.9" for lang in langs[1:])])


def locale_env(locale: str, base_env: Mapping[str, str] | None = None) -> dict[str, str]:
    """Process environment whose POSIX locale matches ``locale`` (drives ICU / ``Intl``)."""
    tag = normalize_locale(locale)
    posix = tag.replace("-", "_")
    env = dict(os.environ if base_env is None else base_env)
    env["LANG"] = f"{posix}.UTF-8"
    env["LANGUAGE"] = ":".join(lang.replace("-", "_") for lang in accept_languages(tag))
    env.pop("LC_ALL", None)  # would override LANG for every category
    return env


def screen_for_viewport(width: int, height: int) -> tuple[int, int]:
    """Smallest common desktop screen that can hold the viewport plus browser UI."""
    for screen_w, screen_h in COMMON_SCREENS:
        if screen_w >= width and screen_h >= height + BROWSER_UI_HEIGHT:
            return screen_w, screen_h
    return width, height + BROWSER_UI_HEIGHT


def user_agent_for(browser_version: str, system: str | None = None) -> str:
    """Reduced Chrome UA for ``browser_version`` on the host OS.

    Only needed in headless mode, where Chromium advertises ``HeadlessChrome``. The
    platform token follows the *real* OS so it agrees with ``navigator.platform``,
    ``Sec-CH-UA-Platform`` and the WebGL renderer — claiming Windows from a Linux box
    would contradict every one of them.
    """
    major = (browser_version or "").split(".", 1)[0]
    if not major.isdigit():
        raise ValueError(f"unexpected browser version {browser_version!r}")
    token = _UA_PLATFORM.get(system or _platform.system(), _UA_PLATFORM["Linux"])
    return f"Mozilla/5.0 ({token}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{major}.0.0.0 Safari/537.36"


def parse_proxy(proxy: str) -> dict[str, str]:
    """``scheme://user:pass@host:port`` -> Playwright proxy settings."""
    raw = proxy.strip()
    if "://" not in raw:
        raw = f"http://{raw}"
    parts = urlsplit(raw)
    if not parts.hostname:
        raise ValueError(f"invalid proxy URL {proxy!r}")
    host = parts.hostname if ":" not in parts.hostname else f"[{parts.hostname}]"
    server = f"{parts.scheme}://{host}" + (f":{parts.port}" if parts.port else "")
    settings = {"server": server}
    if parts.username:
        settings["username"] = unquote(parts.username)
    if parts.password:
        settings["password"] = unquote(parts.password)
    return settings


def build_context_options(cfg: "BrowserSection", geo: "GeoPin", user_agent: str | None = None) -> dict[str, Any]:
    """Keyword arguments for ``browser.new_context`` describing one coherent device.

    **Consistency beats randomisation.** Anti-bot systems score *contradictions*
    between independent signals far more than any single value. The egress IP
    geolocates to a city, so the emulated ``geolocation`` must be that city (``geo``);
    ``timezone_id`` must be its zone (``Date`` and ``Intl`` leak it); the locale must
    fit the region, and ``navigator.language(s)`` and the ``Accept-Language`` header
    must agree with it. A viewport comes with a larger, common ``screen`` so
    ``screen.width >= innerWidth`` holds like on a real desktop. Run the collector on
    the operator's home connection (or a residential proxy in the same metro) so the
    IP completes the picture.

    The locale itself is configured on the *browser process* by :func:`launch_options`
    (``--lang``/``--accept-lang`` + ``LANG``), not with Playwright's ``locale``
    context option: that option overrides ``Accept-Language`` with a bare ``en-US``
    on navigations while ``extra_http_headers`` only reach sub-resources, so a page
    and its own XHRs would advertise different language lists. The native switches
    give one value for documents, XHR/fetch and ``navigator.languages``.

    The User-Agent is *not* rotated. A logged-in session's cookies were issued to the
    device fingerprint that performed the login; presenting a different UA (or
    screen, timezone...) with the same cookies looks like a hijacked session and
    burns the trust the account has accumulated, which is exactly what triggers
    checkpoints. Pass ``user_agent`` only to remove the ``HeadlessChrome`` token in
    headless mode (see :func:`user_agent_for`); it then stays identical for the
    whole life of the session.

    ``storage_state`` is included when the file exists, so a context starts with the
    cookies saved by the login CLI.
    """
    screen_w, screen_h = screen_for_viewport(cfg.viewport_width, cfg.viewport_height)
    options: dict[str, Any] = {
        "viewport": {"width": cfg.viewport_width, "height": cfg.viewport_height},
        "screen": {"width": screen_w, "height": screen_h},
        "device_scale_factor": 1,
        "is_mobile": False,
        "has_touch": False,
        "timezone_id": geo.timezone_id,
        "geolocation": {"latitude": geo.latitude, "longitude": geo.longitude, "accuracy": GEO_ACCURACY_METERS},
        "permissions": ["geolocation"],
        "color_scheme": "light",
        "reduced_motion": "no-preference",
        "java_script_enabled": True,
        "accept_downloads": False,
    }
    if user_agent:
        options["user_agent"] = user_agent
    state_path = Path(cfg.storage_state_path).expanduser() if cfg.storage_state_path else None
    if state_path is not None and state_path.is_file():
        options["storage_state"] = str(state_path)
    return options


def launch_options(cfg: "BrowserSection", *, locale: str | None = None) -> dict[str, Any]:
    """Keyword arguments for ``chromium.launch`` / ``launch_persistent_context``.

    Without an explicit ``executable_path`` headless runs use the ``chromium`` channel,
    i.e. the full browser in Chrome's *new* headless mode, which shares the rendering
    stack (plugins, ``window.chrome``, codecs) of headed Chrome — unlike the separate
    ``chrome-headless-shell`` binary Playwright uses by default.

    ``locale`` sets the UI language, the accept-language list and the process locale
    together (see :func:`build_context_options` for why this is not done per context).
    """
    window_h = cfg.viewport_height + BROWSER_UI_HEIGHT - 35
    args = [*BROWSER_ARGS, f"--window-size={cfg.viewport_width},{window_h}"]
    options: dict[str, Any] = {
        "headless": cfg.headless,
        "args": args,
        "ignore_default_args": list(IGNORED_DEFAULT_ARGS),
    }
    if locale:
        tag = normalize_locale(locale)
        args += [f"--lang={tag}", f"--accept-lang={','.join(accept_languages(tag))}"]
        options["env"] = locale_env(tag)
    if cfg.executable_path:
        options["executable_path"] = cfg.executable_path
    elif cfg.headless:
        options["channel"] = "chromium"
    if cfg.proxy:
        options["proxy"] = parse_proxy(cfg.proxy)
    return options


def _missing_executable(exc: BaseException) -> bool:
    return "executable doesn't exist" in str(exc).lower()


async def launch_browser(playwright: "Playwright", cfg: "BrowserSection", *, locale: str | None = None) -> "Browser":
    """Launch Chromium with the hardened switches (see :func:`launch_options`).

    Falls back from the full-browser ``chromium`` channel to Playwright's default
    headless build when only ``chrome-headless-shell`` is installed; the init script
    then fills in what the shell lacks (plugins, ``window.chrome``).
    """
    options = launch_options(cfg, locale=locale)
    try:
        return await playwright.chromium.launch(**options)
    except PlaywrightError as exc:
        if "channel" not in options or not _missing_executable(exc):
            raise
        log.warning("full chromium build not installed; falling back to headless shell", extra={"error": str(exc)[:200]})
        options.pop("channel")
        return await playwright.chromium.launch(**options)


async def _launch_persistent(playwright: "Playwright", user_data_dir: str, options: dict[str, Any]) -> "BrowserContext":
    try:
        return await playwright.chromium.launch_persistent_context(user_data_dir, **options)
    except PlaywrightError as exc:
        if "channel" not in options or not _missing_executable(exc):
            raise
        log.warning("full chromium build not installed; falling back to headless shell", extra={"error": str(exc)[:200]})
        options = {k: v for k, v in options.items() if k != "channel"}
        return await playwright.chromium.launch_persistent_context(user_data_dir, **options)


async def new_stealth_context(
    playwright: "Playwright",
    cfg: "BrowserSection",
    geo: "GeoPin",
    *,
    use_storage_state: bool = True,
) -> tuple["Browser | None", "BrowserContext"]:
    """Launch a browser and open one hardened context (init script, timeouts, identity).

    With ``cfg.user_data_dir`` a persistent profile is used (``browser`` is then
    ``None``; closing the context closes the browser). Otherwise a regular browser +
    context is created, seeded from ``cfg.storage_state_path`` when it exists.
    """
    timeout_ms = cfg.navigation_timeout_seconds * 1000.0
    browser: Browser | None = None
    if cfg.user_data_dir:
        user_agent = None
        if cfg.headless:
            # A persistent context exposes no Browser before it exists, and the UA must
            # be fixed at creation: read the version from a short-lived probe launch.
            probe = await launch_browser(playwright, cfg, locale=geo.locale)
            try:
                user_agent = user_agent_for(probe.version)
            finally:
                await probe.close()
        options = build_context_options(cfg, geo, user_agent)
        options.pop("storage_state", None)  # the profile directory holds the cookies
        profile_dir = Path(cfg.user_data_dir).expanduser()
        await asyncio.to_thread(profile_dir.mkdir, mode=0o700, parents=True, exist_ok=True)
        launch = launch_options(cfg, locale=geo.locale)
        context = await _launch_persistent(playwright, str(profile_dir), {**launch, **options})
    else:
        browser = await launch_browser(playwright, cfg, locale=geo.locale)
        try:
            user_agent = user_agent_for(browser.version) if cfg.headless else None
            options = build_context_options(cfg, geo, user_agent)
            if not use_storage_state:
                options.pop("storage_state", None)
            context = await browser.new_context(**options)
        except BaseException:
            with contextlib.suppress(Exception):
                await browser.close()
            raise
    try:
        context.set_default_navigation_timeout(timeout_ms)
        context.set_default_timeout(timeout_ms)
        await context.add_init_script(STEALTH_INIT_SCRIPT)
    except BaseException:
        with contextlib.suppress(Exception):
            await context.close()
        if browser is not None:
            with contextlib.suppress(Exception):
                await browser.close()
        raise
    return browser, context


# --------------------------------------------------------------------------- pacing


async def human_pause(rng: random.Random, min_s: float, max_s: float) -> float:
    """Sleep a right-skewed random time in ``[min_s, max_s]`` and return it.

    Beta(2, 3) puts most pauses in the lower half with an occasional long "reading"
    pause — closer to human think time than a uniform draw.
    """
    lo, hi = sorted((max(0.0, float(min_s)), max(0.0, float(max_s))))
    delay = lo + (hi - lo) * rng.betavariate(2.0, 3.0)
    if delay > 0:
        await asyncio.sleep(delay)
    return delay


async def human_scroll(
    page: "Page",
    rng: random.Random,
    steps: int,
    *,
    min_pause: float = 0.35,
    max_pause: float = 1.3,
) -> int:
    """Scroll with mouse-wheel events: ``steps`` uneven flicks, each split into ticks.

    The pointer first drifts to a random spot in the middle of the viewport (wheel
    events target the element under the cursor). Occasionally a flick goes back up a
    little, as people do when they overshoot. Returns the net scrolled distance.
    """
    viewport = page.viewport_size or {"width": 1366, "height": 900}
    x = viewport["width"] * rng.uniform(0.3, 0.7)
    y = viewport["height"] * rng.uniform(0.35, 0.65)
    await page.mouse.move(x, y, steps=rng.randint(4, 12))
    total = 0
    for index in range(max(0, steps)):
        delta = rng.randint(280, 760)
        if index and rng.random() < 0.12:
            delta = -rng.randint(60, 220)
        ticks = rng.randint(2, 4)
        for _ in range(ticks):
            await page.mouse.wheel(0, delta / ticks)
            await asyncio.sleep(rng.uniform(0.02, 0.09))
        total += delta
        await human_pause(rng, min_pause, max_pause)
    return total


# --------------------------------------------------------------------------- session state


def write_private_json(path: Path, data: Any) -> None:
    """Atomically write ``data`` as JSON readable only by the owner (0600).

    The file holds live session cookies, i.e. account credentials: it is written to a
    temp file in the same directory (``mkstemp`` creates it 0600), fsynced and swapped
    in with ``os.replace`` so a crash never leaves a truncated state file behind.
    """
    target = path.expanduser()
    parent = target.parent
    if not parent.exists():
        parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", suffix=".tmp", dir=str(parent))
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(tmp_name, 0o600)
        os.replace(tmp_name, target)
    except BaseException:
        with contextlib.suppress(FileNotFoundError):
            os.unlink(tmp_name)
        raise
    if hasattr(os, "O_DIRECTORY"):
        with contextlib.suppress(OSError):
            dir_fd = os.open(str(parent), os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)


async def save_storage_state(context: "BrowserContext", path: str | Path) -> Path:
    """Persist the context's cookies + localStorage atomically with 0600 permissions."""
    state = await context.storage_state()
    target = Path(path).expanduser()
    await asyncio.to_thread(write_private_json, target, state)
    return target


__all__ = [
    "BROWSER_ARGS",
    "COMMON_SCREENS",
    "GEO_ACCURACY_METERS",
    "IGNORED_DEFAULT_ARGS",
    "STEALTH_INIT_SCRIPT",
    "accept_language",
    "accept_languages",
    "build_context_options",
    "human_pause",
    "human_scroll",
    "launch_browser",
    "launch_options",
    "locale_env",
    "new_stealth_context",
    "normalize_locale",
    "parse_proxy",
    "save_storage_state",
    "screen_for_viewport",
    "user_agent_for",
    "write_private_json",
]
