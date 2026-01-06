# app/auth/rbac.py

from typing import Awaitable, Callable, Optional
from fastapi import Request
from tessera_sdk.utils.authorization_dependency import authorize

DomainResolver = Callable[[Request], Awaitable[Optional[str]]]

PREFIX = "eventa"


class RBACActions:
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"


def build_rbac_dependencies(
    *,
    resource: str,
    domain_resolver: DomainResolver,
):
    return {
        "create": authorize(
            resource=f"{PREFIX}.{resource}",
            action=RBACActions.CREATE,
            domain_resolver=domain_resolver,
        ),
        "read": authorize(
            resource=f"{PREFIX}.{resource}",
            action=RBACActions.READ,
            domain_resolver=domain_resolver,
        ),
        "update": authorize(
            resource=f"{PREFIX}.{resource}",
            action=RBACActions.UPDATE,
            domain_resolver=domain_resolver,
        ),
        "delete": authorize(
            resource=f"{PREFIX}.{resource}",
            action=RBACActions.DELETE,
            domain_resolver=domain_resolver,
        ),
    }
