import { useEffect, useRef, memo } from 'react';

/**
 * TradingView Advanced Chart Widget Component
 *
 * Embeds TradingView's Advanced Chart with pre-loaded technical indicators:
 * - Simple Moving Average (20, 50, 200)
 * - MACD
 * - Stochastic Oscillator
 *
 * @param {Object} props
 * @param {string} props.symbol - Stock ticker symbol (e.g., "BBCA")
 */
function TradingViewChart({ symbol }) {
    const container = useRef(null);

    useEffect(() => {
        if (!symbol || !container.current) return;

        // Clear previous widget
        container.current.innerHTML = '';

        // Create widget container
        const widgetContainer = document.createElement('div');
        widgetContainer.className = 'tradingview-widget-container__widget';
        widgetContainer.style.height = 'calc(100% - 32px)';
        widgetContainer.style.width = '100%';
        container.current.appendChild(widgetContainer);

        // Create copyright notice (required for free usage)
        const copyright = document.createElement('div');
        copyright.className = 'tradingview-widget-copyright';
        copyright.innerHTML = `
            <a href="https://www.tradingview.com/symbols/IDX-${symbol}/"
               rel="noopener nofollow"
               target="_blank">
                <span class="blue-text">${symbol} chart</span>
            </a>
            <span class="trademark"> by TradingView</span>
        `;
        container.current.appendChild(copyright);

        // Create and configure the script
        const script = document.createElement('script');
        script.src = 'https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js';
        script.type = 'text/javascript';
        script.async = true;
        script.innerHTML = JSON.stringify({
            autosize: true,
            symbol: `IDX:${symbol}`,
            interval: 'D',
            timezone: 'Asia/Jakarta',
            theme: 'dark',
            style: '1', // Candlestick
            locale: 'en',
            backgroundColor: 'rgba(19, 23, 34, 1)',
            gridColor: 'rgba(42, 46, 57, 0.3)',
            withdateranges: true,
            hide_side_toolbar: false,
            allow_symbol_change: false,
            save_image: false,
            calendar: false,
            studies: [
                'MACD@tv-basicstudies',
                'STOCHastic@tv-basicstudies',
                { id: 'MASimple@tv-basicstudies', inputs: { length: 20 } },
                { id: 'MASimple@tv-basicstudies', inputs: { length: 50 } },
                { id: 'MASimple@tv-basicstudies', inputs: { length: 200 } }
            ],
            show_popup_button: true,
            popup_width: '1000',
            popup_height: '650',
            support_host: 'https://www.tradingview.com'
        });

        container.current.appendChild(script);

        // Cleanup on unmount or symbol change
        return () => {
            if (container.current) {
                container.current.innerHTML = '';
            }
        };
    }, [symbol]);

    if (!symbol) {
        return (
            <div className="chart-placeholder">
                <p>Select a ticker to view the chart</p>
            </div>
        );
    }

    return (
        <div
            className="tradingview-widget-container"
            ref={container}
            style={{ height: '100%', width: '100%' }}
        />
    );
}

export default memo(TradingViewChart);
