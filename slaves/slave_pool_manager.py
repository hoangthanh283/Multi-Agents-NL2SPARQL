import json
import threading
from typing import Any, Dict, List, Optional

from prometheus_client import Gauge

import utils.monitoring
from slaves.slave_pool import SlavePool
from utils.logging_utils import setup_logging

logger = setup_logging(app_name="nl-to-sparql", enable_colors=True)

# Slave type definitions for each domain
SLAVE_TYPES = {
    "nlp": [
        {
            "type": "query_refinement", 
            "class_path": "slaves.nlp.query_refinement_slave.QueryRefinementSlave"
        },
        {
            "type": "entity_recognition", 
            "class_path": "slaves.nlp.entity_recognition_slave.EntityRecognitionSlave"
        }
    ],
    "query": [
        {
            "type": "ontology_mapping", 
            "class_path": "slaves.query.ontology_mapping_slave.OntologyMappingSlave"
        },
        {
            "type": "sparql_construction", 
            "class_path": "slaves.query.sparql_construction_slave.SparqlConstructionSlave"
        },
        {
            "type": "validation", 
            "class_path": "slaves.query.validation_slave.ValidationSlave"
        }
    ],
    "response": [
        {
            "type": "query_execution", 
            "class_path": "slaves.response.query_execution_slave.QueryExecutionSlave"
        },
        {
            "type": "response_generation", 
            "class_path": "slaves.response.response_generation_slave.ResponseGenerationSlave"
        }
    ]
}

class SlavePoolManager:
    """
    Manager for all slave pools across domains.
    Initializes and coordinates the slave pools required by the system.
    """
    
    def __init__(self, redis_url: str, pool_configs: Dict[str, Any] = None):
        """
        Initialize the slave pool manager.
        
        Args:
            redis_url: Redis connection URL
            pool_configs: Configuration for each slave pool
        """
        self.redis_url = redis_url
        self.pool_configs = pool_configs or {}
        
        # Initialize pools
        self.pools: Dict[str, Dict[str, SlavePool]] = {
            "nlp": {},
            "query": {},
            "response": {}
        }
        
        # Track all running pools
        self.running = False
        self.pool_lock = threading.Lock()
        
        # Prometheus metrics
        self.total_pools = Gauge(
            'total_slave_pools',
            'Total number of slave pools',
            ['domain']
        )
        self.active_pools = Gauge(
            'active_slave_pools',
            'Number of active slave pools',
            ['domain']
        )
        self.pool_size = Gauge(
            'slave_pool_current_size',
            'Current number of slaves in a pool',
            ['domain', 'slave_type']
        )
        self.pool_capacity = Gauge(
            'slave_pool_capacity',
            'Maximum capacity of a slave pool',
            ['domain', 'slave_type']
        )
        
        # Initialize metrics for each domain
        for domain in self.pools:
            self.total_pools.labels(domain=domain).set(0)
            self.active_pools.labels(domain=domain).set(0)
        
        logger.info("SlavePoolManager initialized")
    
    def start_pools(self, domains: List[str] = None, slave_types: Dict[str, List[str]] = None):
        """
        Start slave pools for specified domains and types.
        
        Args:
            domains: List of domains to start pools for. If None, start all domains.
            slave_types: Dict mapping domains to list of slave types to start.
                        If None, start all slave types for the specified domains.
        """
        if not domains:
            domains = list(SLAVE_TYPES.keys())
            
        for domain in domains:
            if domain not in SLAVE_TYPES:
                logger.error(f"Unknown domain: {domain}")
                continue
                
            domain_slave_types = []
            if slave_types and domain in slave_types:
                # Only start specific slave types for this domain
                for slave_info in SLAVE_TYPES[domain]:
                    if slave_info["type"] in slave_types[domain]:
                        domain_slave_types.append(slave_info)
            else:
                # Start all slave types for this domain
                domain_slave_types = SLAVE_TYPES[domain]
                
            for slave_info in domain_slave_types:
                self._start_slave_pool(domain, slave_info["type"], slave_info["class_path"])
                
            # Update metrics
            self.total_pools.labels(domain=domain).set(len(domain_slave_types))
            
        self.running = True
        logger.info(f"Started slave pools for domains: {', '.join(domains)}")
        
        # Start monitoring thread for pool metrics
        self.monitoring_thread = threading.Thread(target=self._monitor_pools, daemon=True)
        self.monitoring_thread.start()
    
    def _monitor_pools(self):
        """Monitor pool metrics in a background thread"""
        while self.running:
            self._update_pool_metrics()
            # Sleep for 5 seconds between updates
            threading.Event().wait(5)
            
    def _update_pool_metrics(self):
        """Update metrics for all pools"""
        try:
            for domain in self.pools:
                for slave_type, pool in self.pools[domain].items():
                    try:
                        status = pool.get_status()
                        current_size = status.get("current_size", 0)
                        max_size = status.get("max_size", 0)
                        
                        # Update Prometheus metrics
                        self.pool_size.labels(domain=domain, slave_type=slave_type).set(current_size)
                        self.pool_capacity.labels(domain=domain, slave_type=slave_type).set(max_size)
                        
                        # Log to monitoring system
                        utils.monitoring.log_slave_pool_size(domain, slave_type, current_size)
                    except Exception as e:
                        logger.error(f"Error updating metrics for {domain}/{slave_type}: {e}")
        except Exception as e:
            logger.error(f"Error in pool metrics monitoring: {e}")
    
    def _start_slave_pool(self, domain: str, slave_type: str, class_path: str):
        """
        Start a specific slave pool.
        
        Args:
            domain: Domain name
            slave_type: Type of slave
            class_path: Import path to the slave class
        """
        with self.pool_lock:
            # Check if pool already exists
            if slave_type in self.pools[domain]:
                logger.warning(f"Slave pool for {domain}/{slave_type} already exists")
                return
                
            # Get configuration for this pool
            pool_config_key = f"{domain}.{slave_type}"
            pool_config = self.pool_configs.get(pool_config_key, {})
            
            # Set defaults if not provided
            initial_size = pool_config.get("initial_size", 2)
            max_size = pool_config.get("max_size", 10)
            slave_config = pool_config.get("slave_config", {})
            
            # Create a separate registry for each slave type to avoid conflicts
            from prometheus_client import CollectorRegistry
            registry = CollectorRegistry()
            
            # Deep copy slave_config to avoid modifying the original
            import copy
            slave_config_copy = copy.deepcopy(slave_config)
            
            # Add registry to slave config
            slave_config_copy["registry"] = registry
            
            try:
                # Create and start the pool
                pool = SlavePool(
                    domain=domain,
                    slave_type=slave_type,
                    slave_class_path=class_path,
                    redis_url=self.redis_url,
                    initial_size=initial_size,
                    max_size=max_size,
                    slave_config=slave_config_copy  # Use the copy with the registry
                )
                pool.start()
                
                # Add to our pools
                self.pools[domain][slave_type] = pool
                self.active_pools.labels(domain=domain).inc()
                
                # Set initial metrics
                self.pool_size.labels(domain=domain, slave_type=slave_type).set(initial_size)
                self.pool_capacity.labels(domain=domain, slave_type=slave_type).set(max_size)
                
                # Log to monitoring system
                utils.monitoring.log_slave_pool_size(domain, slave_type, initial_size)
                
                logger.info(f"Started slave pool for {domain}/{slave_type}")
                
            except Exception as e:
                logger.error(f"Error starting slave pool for {domain}/{slave_type}: {e}")
    
    def stop_all_pools(self):
        """Stop all running slave pools."""
        if not self.running:
            return
            
        self.running = False  # Signal monitoring thread to stop
        
        with self.pool_lock:
            for domain in self.pools:
                for slave_type, pool in self.pools[domain].items():
                    try:
                        pool.stop()
                        
                        # Reset metrics
                        self.pool_size.labels(domain=domain, slave_type=slave_type).set(0)
                        self.pool_capacity.labels(domain=domain, slave_type=slave_type).set(0)
                        
                        logger.info(f"Stopped slave pool for {domain}/{slave_type}")
                    except Exception as e:
                        logger.error(f"Error stopping slave pool for {domain}/{slave_type}: {e}")
                        
                # Reset domain metrics
                self.active_pools.labels(domain=domain).set(0)
                
            # Clear pools
            for domain in self.pools:
                self.pools[domain].clear()
                
        # Wait for monitoring thread to finish
        if hasattr(self, 'monitoring_thread') and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=2.0)
            
        logger.info("Stopped all slave pools")
    
    def stop_domain_pools(self, domain: str):
        """
        Stop all slave pools for a specific domain.
        
        Args:
            domain: Domain name
        """
        if domain not in self.pools:
            logger.warning(f"Unknown domain: {domain}")
            return
            
        with self.pool_lock:
            for slave_type, pool in self.pools[domain].items():
                try:
                    pool.stop()
                    
                    # Reset metrics
                    self.pool_size.labels(domain=domain, slave_type=slave_type).set(0)
                    self.pool_capacity.labels(domain=domain, slave_type=slave_type).set(0)
                    
                    logger.info(f"Stopped slave pool for {domain}/{slave_type}")
                except Exception as e:
                    logger.error(f"Error stopping slave pool for {domain}/{slave_type}: {e}")
            
            # Clear pools for this domain
            self.pools[domain].clear()
            self.active_pools.labels(domain=domain).set(0)
            
        logger.info(f"Stopped all slave pools for domain: {domain}")
    
    def get_pool(self, domain: str, slave_type: str) -> Optional[SlavePool]:
        """
        Get a specific slave pool.
        
        Args:
            domain: Domain name
            slave_type: Type of slave
            
        Returns:
            SlavePool instance or None if not found
        """
        if domain not in self.pools or slave_type not in self.pools[domain]:
            return None
            
        return self.pools[domain][slave_type]
    
    def get_status(self) -> Dict[str, Any]:
        """
        Get the status of all slave pools.
        
        Returns:
            Dictionary with status information for all pools
        """
        status = {}
        
        for domain in self.pools:
            domain_status = {}
            for slave_type, pool in self.pools[domain].items():
                try:
                    domain_status[slave_type] = pool.get_status()
                except Exception as e:
                    domain_status[slave_type] = {
                        "status": "error",
                        "error": str(e)
                    }
            
            status[domain] = domain_status
            
        return status
    
    def get_health(self) -> Dict[str, bool]:
        """
        Get the health status of all slave pools.
        
        Returns:
            Dictionary mapping domains to health status
        """
        health = {}
        
        for domain in self.pools:
            # Domain is healthy if at least one pool is healthy
            domain_healthy = False
            for slave_type, pool in self.pools[domain].values():
                try:
                    if pool.get_health():
                        domain_healthy = True
                        break
                except Exception:
                    pass
                    
            health[domain] = domain_healthy
            
        return health
        
    def scale_pool(self, domain: str, slave_type: str, target_size: int) -> bool:
        """
        Scale a slave pool to a target size.
        
        Args:
            domain: Domain name
            slave_type: Type of slave
            target_size: Target number of slaves in pool
            
        Returns:
            True if successful, False otherwise
        """
        pool = self.get_pool(domain, slave_type)
        if not pool:
            logger.error(f"No pool found for {domain}/{slave_type}")
            return False
            
        success = pool.scale(target_size)
        
        if success:
            # Update metrics
            self.pool_size.labels(domain=domain, slave_type=slave_type).set(target_size)
            utils.monitoring.log_slave_pool_size(domain, slave_type, target_size)
            
        return success