(function () {
    var pendingTimers = [];
    var lastFallbackHeight = 0;
    var isBxReady = false;
    var isBxInitStarted = false;
    var readyCallbacks = [];
    var scheduledFrame = 0;
    var observerDelayTimer = 0;
    var fitRunId = 0;

    function clearPendingTimers() {
        while (pendingTimers.length) {
            window.clearTimeout(pendingTimers.pop());
        }
    }

    function getRootElement() {
        return document.querySelector(".page-shell") || document.body;
    }

    function measureHeight() {
        var root = getRootElement();
        var docEl = document.documentElement;
        var body = document.body;

        return Math.max(
            root ? Math.ceil(root.scrollHeight || 0) : 0,
            root ? Math.ceil(root.offsetHeight || 0) : 0,
            docEl ? Math.ceil(docEl.scrollHeight || 0) : 0,
            docEl ? Math.ceil(docEl.offsetHeight || 0) : 0,
            body ? Math.ceil(body.scrollHeight || 0) : 0,
            body ? Math.ceil(body.offsetHeight || 0) : 0,
            180
        );
    }

    function flushReadyCallbacks() {
        while (readyCallbacks.length) {
            try {
                readyCallbacks.shift()();
            } catch (error) {
                console.debug("BX24 ready callback failed", error);
            }
        }
    }

    function withBxReady(callback) {
        if (!window.BX24) {
            return;
        }

        if (isBxReady || typeof BX24.init !== "function") {
            isBxReady = true;
            callback();
            return;
        }

        readyCallbacks.push(callback);

        if (isBxInitStarted) {
            return;
        }

        isBxInitStarted = true;
        BX24.init(function () {
            isBxReady = true;
            flushReadyCallbacks();
        });
    }

    function fitWindow() {
        withBxReady(function () {
            try {
                if (typeof BX24.fitWindow === "function") {
                    BX24.fitWindow();
                    return;
                }

                if (typeof BX24.resizeWindow === "function") {
                    var width = Math.max(
                        Math.ceil(document.documentElement.clientWidth || 0),
                        320
                    );
                    var height = measureHeight();

                    if (Math.abs(height - lastFallbackHeight) <= 1) {
                        return;
                    }

                    lastFallbackHeight = height;
                    BX24.resizeWindow(width, height);
                }
            } catch (error) {
                console.debug("BX24 resize skipped", error);
            }
        });
    }

    function scheduleObservedFit() {
        if (observerDelayTimer) {
            window.clearTimeout(observerDelayTimer);
        }

        observerDelayTimer = window.setTimeout(function () {
            observerDelayTimer = 0;
            scheduleFit();
        }, 40);
    }

    function scheduleFit() {
        fitRunId += 1;
        var currentRunId = fitRunId;

        clearPendingTimers();
        if (observerDelayTimer) {
            window.clearTimeout(observerDelayTimer);
            observerDelayTimer = 0;
        }
        if (scheduledFrame) {
            window.cancelAnimationFrame(scheduledFrame);
        }

        scheduledFrame = window.requestAnimationFrame(function () {
            scheduledFrame = 0;
            fitWindow();

            [120, 320, 700].forEach(function (delay) {
                pendingTimers.push(window.setTimeout(function () {
                    if (currentRunId !== fitRunId) {
                        return;
                    }
                    fitWindow();
                }, delay));
            });
        });
    }

    function normalizeLang(value) {
        var normalized = String(value || "").toLowerCase().replace("_", "-");
        if (normalized.indexOf("en") === 0) {
            return "en";
        }
        return "de";
    }

    function updateLanguageInputs(rawPortalLang) {
        Array.prototype.forEach.call(document.querySelectorAll('input[name="auth[lang]"]'), function (input) {
            input.value = rawPortalLang || normalizeLang(rawPortalLang);
        });
    }

    function syncPortalLanguage() {
        var body = document.body;
        if (!body || !window.BX24 || typeof BX24.getLang !== "function") {
            return;
        }

        var currentAppLang = normalizeLang(body.getAttribute("data-app-lang"));
        var languageMode = String(body.getAttribute("data-language-mode") || "auto").toLowerCase();

        withBxReady(function () {
            var rawPortalLang = "";

            try {
                rawPortalLang = BX24.getLang() || "";
            } catch (error) {
                console.debug("BX24.getLang unavailable", error);
                return;
            }

            if (!rawPortalLang) {
                return;
            }

            updateLanguageInputs(rawPortalLang);

            if (body.classList.contains("auth-bootstrap")) {
                return;
            }

            if (languageMode !== "auto") {
                return;
            }

            if (normalizeLang(rawPortalLang) === currentAppLang) {
                return;
            }

            try {
                var url = new URL(window.location.href);
                url.searchParams.set("app_lang", rawPortalLang);
                window.location.replace(url.toString());
            } catch (error) {
                console.debug("Portal language sync skipped", error);
            }
        });
    }

    document.addEventListener("DOMContentLoaded", scheduleFit);
    document.addEventListener("DOMContentLoaded", syncPortalLanguage);
    window.addEventListener("load", scheduleFit);
    window.addEventListener("resize", scheduleObservedFit);

    if (window.ResizeObserver) {
        var root = getRootElement();

        if (root) {
            new ResizeObserver(scheduleObservedFit).observe(root);
        }
    }

    if (window.MutationObserver && document.body) {
        new MutationObserver(scheduleObservedFit).observe(document.body, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeFilter: ["class", "hidden", "style", "aria-hidden"]
        });
    }
})();
