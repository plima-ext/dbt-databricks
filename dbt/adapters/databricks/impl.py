from dataclasses import dataclass
from typing import Optional, List, Dict, Union, Set

from dbt.adapters.base import AdapterConfig
from dbt.adapters.databricks import DatabricksConnectionManager
from dbt.adapters.databricks.relation import DatabricksRelation
from dbt.adapters.databricks.column import DatabricksColumn

from dbt.adapters.spark.impl import SparkAdapter

from dbt.contracts.graph.manifest import Manifest

@dataclass
class DatabricksConfig(AdapterConfig):
    file_format: str = "delta"
    location_root: Optional[str] = None
    partition_by: Optional[Union[List[str], str]] = None
    clustered_by: Optional[Union[List[str], str]] = None
    buckets: Optional[int] = None
    options: Optional[Dict[str, str]] = None
    merge_update_columns: Optional[str] = None


class DatabricksAdapter(SparkAdapter):

    Relation = DatabricksRelation
    Column = DatabricksColumn

    ConnectionManager = DatabricksConnectionManager
    connections: DatabricksConnectionManager

    AdapterSpecificConfigs = DatabricksConfig

    def _get_cache_schemas(self, manifest: Manifest) -> Set[BaseRelation]:
        """Get the set of schema relations that the cache logic needs to
        populate. This means only executable nodes are included.
        """
        # the cache only cares about executable nodes
        relations = [
            self.Relation.create_from(self.config, node)  # keep the identifier
            for node in manifest.nodes.values()
            if (
                node.is_relational and not node.is_ephemeral_model
            )
        ]
        # group up relations by common schema
        import collections
        relmap = collections.defaultdict(list)
        for r in relations:
            relmap[r.schema].append(r)
        # create a single relation for each schema
        # set the identifier to a '|' delimited string of relation names, or '*'
        schemas = [
            self.Relation.create(
                schema=schema,                
                identifier=(
                    '|'.join(r.identifier for r in rels)
                    # there's probably some limit to how many we can include by name
                    if len(rels) < 100 else '*'
                )
            ) for schema, rels in relmap.items()
        ]
        return schemas
