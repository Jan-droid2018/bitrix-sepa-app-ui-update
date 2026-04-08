(function () {
    var pendingTimers = [];
    var lastWidth = 0;
    var lastHeight = 0;

    function clearPendingTimers() {
        while (pendingTimers.length) {
            window.clearTimeout(pendingTimers.pop());
        }
    }

    function toNumber(value) {
        var parsed = parseFloat(value);
        return Number.isFinite(parsed) ? parsed : 0;
    }

    function getRootElement() {
        return document.querySelector(".page-shell") || document.body;
    }

    function measureHeight() {
        var root = getRootElement();
        var height = 0;

        if (root) {
            var rect = root.getBoundingClientRect();
            var style = window.getComputedStyle(root);
            height = Math.max(
                Math.ceil(root.offsetTop + root.offsetHeight + toNumber(style.marginBottom)),
                Math.ceil(root.offsetTop + rect.height + toNumber(style.marginBottom))
            );
        }

        if (!height && document.body) {
            Array.prototype.forEach.call(document.body.children, function (node) {
                if (node.tagName === "SCRIPT") {
                    return;
                }

                var nodeRect = node.getBoundingClientRect();
                var nodeStyle = window.getComputedStyle(node);
                height = Math.max(
                    height,
                    Math.ceil(node.offsetTop + nodeRect.height + toNumber(nodeStyle.marginBottom))
                );
            });
        }

        return Math.max(height, 180);
    }

    function measureViewportHeight() {
        return Math.max(
            Math.ceil(document.documentElement.clientHeight || 0),
            Math.ceil(window.innerHeight || 0),
            320
        );
    }

    function measureWidth() {
        return Math.max(
            Math.ceil(document.documentElement.clientWidth || 0),
            document.body ? Math.ceil(document.body.clientWidth || 0) : 0,
            320
        );
    }

    function fitWindow() {
        if (!window.BX24) {
            return;
        }

        try {
            var width = measureWidth();
            var height = Math.max(measureHeight(), measureViewportHeight());

            if (
                Math.abs(width - lastWidth) <= 1 &&
                Math.abs(height - lastHeight) <= 1
            ) {
                return;
            }

            if (typeof BX24.resizeWindow === "function") {
                lastWidth = width;
                lastHeight = height;
                BX24.resizeWindow(width, height);
                return;
            }

            if (typeof BX24.fitWindow === "function") {
                BX24.fitWindow();
            }
        } catch (error) {
            console.debug("BX24 resize skipped", error);
        }
    }

    function scheduleFit() {
        clearPendingTimers();

        window.requestAnimationFrame(function () {
            fitWindow();

            [120, 320, 700].forEach(function (delay) {
                pendingTimers.push(window.setTimeout(fitWindow, delay));
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
        if (!body || !window.BX24 || typeof BX24.init !== "function" || typeof BX24.getLang !== "function") {
            return;
        }

        var currentAppLang = normalizeLang(body.getAttribute("data-app-lang"));
        var languageMode = String(body.getAttribute("data-language-mode") || "auto").toLowerCase();

        BX24.init(function () {
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
    window.addEventListener("resize", scheduleFit);

    if (window.ResizeObserver) {
        var root = getRootElement();

        if (root) {
            new ResizeObserver(scheduleFit).observe(root);
        }
    }

    if (window.MutationObserver && document.body) {
        new MutationObserver(scheduleFit).observe(document.body, {
            childList: true,
            subtree: true,
            attributes: true
        });
    }
})();
