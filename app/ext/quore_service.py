import httpx
from app.config import get_settings


class QuoreService:
    """Service for interacting with the Quore API."""

    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.quore_api_url.rstrip("/")
        self.workspace_id = self.settings.quore_workspace_id

    def _get_headers(self, bearer_token: str) -> dict:
        """
        Create headers with bearer token authentication.

        Args:
            bearer_token: The bearer token for authentication

        Returns:
            dict: Headers with Authorization
        """
        return {"Authorization": f"Bearer {bearer_token}"}

    def create_workspace(self, name: str, bearer_token: str) -> dict:
        """
        Create a new workspace in Quore.

        Args:
            name: The name of the workspace to create
            bearer_token: The bearer token for authentication

        Returns:
            dict: The response from the Quore API

        Raises:
            httpx.HTTPError: If the API request fails
        """
        url = f"{self.base_url}/workspaces"
        payload = {"name": name}
        headers = self._get_headers(bearer_token)

        with httpx.Client() as client:
            response = client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()

    def create_project(
        self, name: str, labels: dict[str, str], bearer_token: str
    ) -> dict:
        """
        Create a new project in the Quore workspace.

        Args:
            name: The name of the project to create
            bearer_token: The bearer token for authentication

        Returns:
            dict: The response from the Quore API

        Raises:
            httpx.HTTPError: If the API request fails
        """
        if not self.workspace_id:
            raise ValueError("QUORE_WORKSPACE_ID is not configured")

        url = f"{self.base_url}/workspaces/{self.workspace_id}/projects"
        payload = {"name": name, "labels": labels}
        headers = self._get_headers(bearer_token)

        with httpx.Client() as client:
            response = client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            return response.json()

    def delete_project(self, project_id: str, bearer_token: str) -> dict:
        """
        Delete a project in the Quore workspace.

        Args:
            project_id: The ID of the project to delete
            bearer_token: The bearer token for authentication

        Returns:
            dict: The response from the Quore API

        Raises:
            httpx.HTTPError: If the API request fails
        """
        url = f"{self.base_url}/projects/{project_id}"
        headers = self._get_headers(bearer_token)

        with httpx.Client() as client:
            response = client.delete(url, headers=headers)
            # Raise for HTTP errors before attempting to parse the body
            response.raise_for_status()

            # DELETE endpoints often return 204 No Content. Avoid parsing JSON when empty.
            if response.status_code == 204 or not response.content:
                return {"status": "deleted"}

            return response.json()

    def get_project(self, project_id: str, bearer_token: str) -> dict:
        """
        Get a project in the Quore workspace.
        """
        url = f"{self.base_url}/projects/{project_id}"
        headers = self._get_headers(bearer_token)
        with httpx.Client() as client:
            response = client.get(url, headers=headers)
            response.raise_for_status()
            return response.json()

    def get_projects(self) -> dict:
        """
        Get all projects in the Quore workspace.

        Returns:
            dict: The response from the Quore API

        Raises:
            httpx.HTTPError: If the API request fails
        """
        if not self.workspace_id:
            raise ValueError("QUORE_WORKSPACE_ID is not configured")

        url = f"{self.base_url}/workspaces/{self.workspace_id}/projects"

        with httpx.Client() as client:
            response = client.get(url)
            response.raise_for_status()
            return response.json()


# Create a singleton instance
quore_service = QuoreService()
