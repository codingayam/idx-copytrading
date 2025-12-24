import { useState, useCallback } from 'react';
import { BrokerTab } from './pages/BrokerTab';
import { TickerTab } from './pages/TickerTab';
import { InsightsTab } from './pages/InsightsTab';
import './App.css';

function App() {
    const [activeTab, setActiveTab] = useState('insights');
    const [navigationTarget, setNavigationTarget] = useState({ broker: null, ticker: null });

    const tabs = [
        { id: 'insights', label: 'Insights', icon: '📊' },
        { id: 'broker', label: 'Broker', icon: '🏢' },
        { id: 'ticker', label: 'Ticker', icon: '📈' },
    ];

    // Cross-tab navigation handlers
    const navigateToBroker = useCallback((brokerCode) => {
        setNavigationTarget({ broker: brokerCode, ticker: null });
        setActiveTab('broker');
    }, []);

    const navigateToTicker = useCallback((symbol) => {
        setNavigationTarget({ broker: null, ticker: symbol });
        setActiveTab('ticker');
    }, []);

    const clearNavigation = useCallback(() => {
        setNavigationTarget({ broker: null, ticker: null });
    }, []);

    const renderTab = () => {
        switch (activeTab) {
            case 'broker':
                return (
                    <BrokerTab
                        initialBroker={navigationTarget.broker}
                        onNavigateToTicker={navigateToTicker}
                        onClearNavigation={clearNavigation}
                    />
                );
            case 'ticker':
                return (
                    <TickerTab
                        initialTicker={navigationTarget.ticker}
                        onNavigateToBroker={navigateToBroker}
                        onClearNavigation={clearNavigation}
                    />
                );
            case 'insights':
            default:
                return <InsightsTab />;
        }
    };

    return (
        <div className="app">
            {/* Header */}
            <header className="header">
                <div className="header-content">
                    <div className="logo">
                        <div className="logo-icon">IDX</div>
                        <div className="logo-text">
                            Copy<span>Trading</span>
                        </div>
                    </div>

                    {/* Tab Navigation */}
                    <nav className="tab-nav">
                        {tabs.map((tab) => (
                            <button
                                key={tab.id}
                                className={`tab-button ${activeTab === tab.id ? 'active' : ''}`}
                                onClick={() => setActiveTab(tab.id)}
                            >
                                <span style={{ marginRight: '6px' }}>{tab.icon}</span>
                                {tab.label}
                            </button>
                        ))}
                    </nav>
                </div>
            </header>

            {/* Main Content */}
            <main className="main-content">
                {renderTab()}
            </main>

            {/* Footer */}
            <footer style={{
                padding: 'var(--spacing-lg)',
                textAlign: 'center',
                color: 'var(--color-text-muted)',
                fontSize: '0.75rem',
                borderTop: '1px solid var(--color-border)',
            }}>
                IDX Copytrading Dashboard • Data crawled from NeoBDM • Last update: {new Date().toLocaleDateString()}
            </footer>
        </div>
    );
}

export default App;

