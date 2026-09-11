/**
 * Community features – inline star ratings and discussion links.
 *
 * Auto-initialises all elements with class `rc-rating-widget` on page load.
 * Each widget requires a `data-pid` attribute with the programme PID.
 */
(function () {
    'use strict';

    function createStarSVG(filled) {
        const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('width', '18');
        svg.setAttribute('height', '18');
        svg.setAttribute('viewBox', '0 0 24 24');
        svg.setAttribute('fill', filled ? 'currentColor' : 'none');
        svg.setAttribute('stroke', 'currentColor');
        svg.setAttribute('stroke-width', '2');
        svg.classList.add('rc-star');
        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', 'M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z');
        svg.appendChild(path);
        return svg;
    }

    function renderWidget(container) {
        const pid = container.dataset.pid;
        if (!pid) return;

        container.innerHTML = '';
        container.classList.add('rc-rating-widget--ready');

        // Stars container
        const starsEl = document.createElement('span');
        starsEl.className = 'rc-stars';
        starsEl.setAttribute('role', 'group');
        starsEl.setAttribute('aria-label', 'Rate this programme');

        for (let i = 1; i <= 5; i++) {
            const btn = document.createElement('button');
            btn.type = 'button';
            btn.className = 'rc-star-btn';
            btn.dataset.score = i;
            btn.setAttribute('aria-label', i + ' star' + (i > 1 ? 's' : ''));
            btn.title = i + ' star' + (i > 1 ? 's' : '');
            btn.appendChild(createStarSVG(false));
            starsEl.appendChild(btn);
        }

        // Summary label
        const summaryEl = document.createElement('span');
        summaryEl.className = 'rc-rating-summary small text-muted ms-2';

        // Discussion link
        const discLink = document.createElement('a');
        discLink.href = '/community/discussions/' + pid;
        discLink.className = 'rc-disc-link btn btn-sm btn-outline-secondary ms-2';
        discLink.textContent = 'Discuss';
        discLink.title = 'View discussion';

        container.appendChild(starsEl);
        container.appendChild(summaryEl);
        container.appendChild(discLink);

        // Fetch existing rating
        fetchRating(pid, container);

        // Click handlers for stars
        starsEl.addEventListener('click', function (e) {
            const btn = e.target.closest('.rc-star-btn');
            if (!btn) return;
            const score = parseInt(btn.dataset.score, 10);
            submitRating(pid, score, container);
        });

        // Hover effects
        starsEl.addEventListener('mouseover', function (e) {
            const btn = e.target.closest('.rc-star-btn');
            if (!btn) return;
            const hoverScore = parseInt(btn.dataset.score, 10);
            highlightStars(container, hoverScore);
        });
        starsEl.addEventListener('mouseleave', function () {
            const current = parseInt(container.dataset.userScore || '0', 10);
            highlightStars(container, current);
        });
    }

    function highlightStars(container, score) {
        const buttons = container.querySelectorAll('.rc-star-btn');
        buttons.forEach(function (btn) {
            const s = parseInt(btn.dataset.score, 10);
            const filled = s <= score;
            btn.innerHTML = '';
            btn.appendChild(createStarSVG(filled));
            btn.classList.toggle('active', filled);
        });
    }

    function updateSummary(container, avg, count) {
        const el = container.querySelector('.rc-rating-summary');
        if (!el) return;
        if (count > 0) {
            el.textContent = avg.toFixed(1) + ' (' + count + ')';
        } else {
            el.textContent = '';
        }
    }

    function fetchRating(pid, container) {
        fetch('/api/community/ratings/' + pid)
            .then(function (r) { return r.json(); })
            .then(function (data) {
                const userScore = data.user_score || 0;
                container.dataset.userScore = userScore;
                highlightStars(container, userScore);
                updateSummary(container, data.average_score || 0, data.total_ratings || 0);
            })
            .catch(function () { /* silent fail */ });
    }

    function submitRating(pid, score, container) {
        fetch('/api/community/ratings/' + pid, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ score: score })
        })
            .then(function (r) {
                if (r.status === 401) {
                    window.location.href = '/community/login';
                    return null;
                }
                return r.json();
            })
            .then(function (data) {
                if (!data) return;
                container.dataset.userScore = data.score;
                highlightStars(container, data.score);
                updateSummary(container, data.average_score || 0, data.total_ratings || 0);
            })
            .catch(function () { /* silent fail */ });
    }

    // Initialise all widgets on page load
    function init() {
        document.querySelectorAll('.rc-rating-widget').forEach(renderWidget);
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
