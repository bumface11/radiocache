// Word cloud for narrowing a search.  Used on the home and search pages.
// The cloud sits behind a toggle button that is enabled once the words have
// loaded, so the page layout never shifts when the data arrives.
(function () {
    const wrap = document.getElementById('rc-wordcloud-wrap');
    const cloud = document.getElementById('rc-wordcloud');
    const panel = document.getElementById('rc-wordcloud-panel');
    const toggle = document.getElementById('rc-wordcloud-toggle');
    if (!wrap || !cloud || !panel || !toggle) { return; }
    const label = toggle.querySelector('.rc-cloud-toggle-label');
    const spinner = toggle.querySelector('.spinner-border');
    const query = wrap.dataset.query || '';
    const category = wrap.dataset.category || '';
    const sort = wrap.dataset.sort || '';
    const OPEN_KEY = 'rc-wordcloud-open';

    function remembered() {
        try { return sessionStorage.getItem(OPEN_KEY) === '1'; } catch (e) { return false; }
    }
    function remember(open) {
        try { sessionStorage.setItem(OPEN_KEY, open ? '1' : '0'); } catch (e) { /* ignore */ }
    }

    function setOpen(open) {
        panel.hidden = !open;
        toggle.setAttribute('aria-expanded', open ? 'true' : 'false');
        label.textContent = open ? 'Hide word cloud' : 'Show word cloud';
    }

    function searchUrl(q) {
        const params = new URLSearchParams({q: q, category: category});
        if (sort && sort !== 'relevance') { params.set('sort', sort); }
        return '/search?' + params.toString();
    }

    // Grey the cloud while the next page loads; the browser still navigates.
    function markBusy() {
        cloud.classList.add('is-loading');
        cloud.setAttribute('aria-busy', 'true');
    }

    document.querySelectorAll('[data-remove-term]').forEach(function (chip) {
        chip.addEventListener('click', function (e) {
            e.preventDefault();
            markBusy();
            const remove = chip.dataset.removeTerm;
            const rest = query.split(/\s+/).filter(function (t) { return t && t !== remove; });
            window.location.href = searchUrl(rest.join(' '));
        });
    });

    toggle.addEventListener('click', function () {
        const open = panel.hidden;
        setOpen(open);
        remember(open);
    });

    // If the cloud was open on the previous page, keep it open (greyed
    // while loading) so a clicked word doesn't make the panel vanish.
    const wasOpen = remembered();
    cloud.classList.add('is-loading');
    cloud.setAttribute('aria-busy', 'true');
    label.textContent = 'Word cloud';
    if (wasOpen) { panel.hidden = false; }

    function finish(hasWords) {
        cloud.classList.remove('is-loading');
        cloud.removeAttribute('aria-busy');
        if (spinner) { spinner.remove(); }
        if (!hasWords) {
            panel.hidden = true;
            label.textContent = 'No common words';
            return;
        }
        toggle.disabled = false;
        setOpen(wasOpen);
    }

    const params = new URLSearchParams({q: query, category: category, limit: '60'});
    fetch('/api/wordcloud?' + params.toString())
        .then(function (r) { return r.ok ? r.json() : {words: []}; })
        .then(function (data) {
            const words = data.words || [];
            if (words.length) {
                const counts = words.map(function (w) { return w.count; });
                const max = Math.max.apply(null, counts);
                const min = Math.min.apply(null, counts);
                const span = Math.log(max + 1) - Math.log(min + 1);
                // Alphabetical order reads as a cloud rather than a ranked list.
                words.slice().sort(function (a, b) { return a.word.localeCompare(b.word); })
                    .forEach(function (w) {
                        const weight = span ? (Math.log(w.count + 1) - Math.log(min + 1)) / span : 0.5;
                        const a = document.createElement('a');
                        a.href = searchUrl((query ? query + ' ' : '') + w.word);
                        a.className = 'rc-cloud-word rc-cloud-w' + Math.round(weight * 4);
                        a.setAttribute('role', 'listitem');
                        a.title = w.count + ' series';
                        a.textContent = w.word;
                        a.addEventListener('click', markBusy);
                        cloud.appendChild(a);
                    });
            }
            finish(words.length > 0);
        })
        .catch(function () { finish(false); });
}());
