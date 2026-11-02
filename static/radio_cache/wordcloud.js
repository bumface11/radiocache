// Word cloud for narrowing a search.  Used on the home and search pages.
(function () {
    const wrap = document.getElementById('rc-wordcloud-wrap');
    const cloud = document.getElementById('rc-wordcloud');
    if (!wrap || !cloud) { return; }
    const query = wrap.dataset.query || '';
    const category = wrap.dataset.category || '';
    const sort = wrap.dataset.sort || '';

    function searchUrl(q) {
        const params = new URLSearchParams({q: q, category: category});
        if (sort && sort !== 'relevance') { params.set('sort', sort); }
        return '/search?' + params.toString();
    }

    document.querySelectorAll('[data-remove-term]').forEach(function (chip) {
        chip.addEventListener('click', function (e) {
            e.preventDefault();
            const remove = chip.dataset.removeTerm;
            const rest = query.split(/\s+/).filter(function (t) { return t && t !== remove; });
            window.location.href = searchUrl(rest.join(' '));
        });
    });

    const params = new URLSearchParams({q: query, category: category, limit: '60'});
    fetch('/api/wordcloud?' + params.toString())
        .then(function (r) { return r.ok ? r.json() : {words: []}; })
        .then(function (data) {
            const words = data.words || [];
            if (!words.length) { return; }
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
                    cloud.appendChild(a);
                });
            wrap.hidden = false;
        })
        .catch(function () { /* cloud is an enhancement; fail silently */ });
}());
