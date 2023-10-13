from typing import Dict, List, Literal, Optional, Set
from pgsync import plugin


class FullnamePlugin(plugin.Plugin):
    name = "AccessControl"

    def transform(self, doc, **kwargs):
        owner_group: Optional[int] = doc.get("owner_group_id")
        modify_shared_with_groups: List[
            Dict[Literal["group_id"], int]
        ] = doc.get("modify_shared_with_groups", list)
        read_shared_with_groups: List[
            Dict[Literal["group_id"], int]
        ] = doc.get("read_shared_with_groups", list)
        groups: Set[int] = set()
        if owner_group is not None:
            groups.add(owner_group)
        groups.update([d["group_id"] for d in modify_shared_with_groups])
        groups.update([d["group_id"] for d in read_shared_with_groups])
        doc["access_control_groups"] = groups
        return doc
