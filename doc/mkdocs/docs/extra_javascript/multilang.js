$(function() {
    $('[data-multilang]').each(function() {
        var $header = $('<div/>').addClass('multilang-header');

        // Replace title attribute with visible title
        var $title = $('<div/>').text(this.title).addClass('multilang-title');
        $(this).removeAttr('title');
        $header.append($title);

        // Create button for each code block
        var $codeBlocks = $(this).find('pre > code'),
            $btnGroup = $('<div/>').addClass('multilang-btn-group');
        $codeBlocks.each(function(index) {
            // Make sure block is highlighted
            if (!$(this).hasClass('hljs')) {
                hljs.highlightBlock(this);
            }

            // Parse language key from code block
            var lang = parseLang(this);

            var $btn = $('<button/>')
                .addClass('btn btn-small btn-neutral lang-' + lang)
                // TODO: Capitalize is not enough for cases like "cpp"
                .text(capitalize(lang))
                .click(activateLang.bind(this, lang));
            $btnGroup.append($btn);

            // Activate first language by default
            if (index === 0) {
                activateLang(lang);
            }
        });
        $header.append($btnGroup);

        // Prepend the header last to save DOM repaints
        $(this).prepend($header);

        function activateLang(lang) {
            $codeBlocks.hide();
            $codeBlocks.filter('.' + lang).css('display', 'block');
            $btnGroup.children()
                .removeClass('btn-info btn-neutral')
                .not('.lang-' + lang).addClass('btn-neutral');
            $btnGroup.find('.lang-' + lang).addClass('btn-info');
        }
    });

    /**
     * Parse language key from code block
     *
     * @param  {Element} el <code> element highlighted by Highlight.js
     *
     * @return {String}     Language key
     */
    function parseLang(el) {
        var className = $(el).attr('class');
        if (!className) return '';

        var classList = className.split(' '),
            lang = '';
        for (var i = 0; i < classList.length; i++) {
            if (classList[i] !== 'hljs') {
                lang = classList[i];
                break;
            }
        }
        return lang;
    }

    function capitalize(string) {
        return string.charAt(0).toUpperCase() + string.slice(1);
    }
});
