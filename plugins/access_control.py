from typing import List
from pgsync import plugin


class FullnamePlugin(plugin.Plugin):
    name = "AccessControl"

    def transform(self, doc, **kwargs):
        owner_group = doc.get("owner_group")
        modify_shared_with_groups = doc.get("modify_shared_with_groups", list)
        read_shared_with_groups = doc.get("read_shared_with_groups", list)
        groups: List[int] = []
        if owner_group is not None:
            groups.append(owner_group)
        groups.extend(modify_shared_with_groups)
        groups.extend(read_shared_with_groups)
        doc["access_control_groups"] = groups
        return doc
