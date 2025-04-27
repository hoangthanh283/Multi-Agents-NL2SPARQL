from prometheus_client import Counter, Gauge, Histogram

_metrics = {}

def get_or_create_counter(name, description, labels=None):
    """Get an existing counter or create a new one if it doesn't exist"""
    if name not in _metrics:
        _metrics[name] = Counter(name, description, labels or [])
    return _metrics[name]

def get_or_create_gauge(name, description, labels=None):
    """Get an existing gauge or create a new one if it doesn't exist"""
    if name not in _metrics:
        _metrics[name] = Gauge(name, description, labels or [])
    return _metrics[name]

def get_or_create_histogram(name, description, labels=None):
    """Get an existing histogram or create a new one if it doesn't exist"""
    if name not in _metrics:
        _metrics[name] = Histogram(name, description, labels or [])
    return _metrics[name]
