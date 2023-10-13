from typing import Any, Dict, List, Literal, Optional, Set
from pgsync import plugin


class FullnamePlugin(plugin.Plugin):
    name = "AccessControl"

    def transform(self, doc:Dict[str, Any], **kwargs):
        m = "_modify_shared_with_groups"
        index_name = ""
        for k in doc.keys():
            if m in k:
                index_name = k[:-len(m)]
                break
        owner_group: Optional[int] = doc.pop("owner_group_id")
        modify_shared_with_groups: List[
            Dict[Literal["group_id"], int]
        ] = doc.pop(f"{index_name}_modify_shared_with_groups", [])
        read_shared_with_groups: List[
            Dict[Literal["group_id"], int]
        ] = doc.pop(f"{index_name}_read_shared_with_groups", [])
        groups: Set[int] = set()
        if owner_group is not None:
            groups.add(owner_group)
        if modify_shared_with_groups is not None:
            groups.update([d["group_id"] for d in modify_shared_with_groups])
        if read_shared_with_groups is not None:
            groups.update([d["group_id"] for d in read_shared_with_groups])
        doc["access_control_groups"] = list(groups)
        return doc
