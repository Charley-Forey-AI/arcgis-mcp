from __future__ import annotations
import os
import re
import copy
import tempfile
from enum import Enum
from arcgis._impl.common._isd import InsensitiveDict
from typing import Any
from arcgis._impl.common._deprecate import deprecated
from arcgis.gis import User
import requests


class DATAACCESSTYPE(Enum):
    """
    Enum for data access types.
    """

    FOLDER = "folder"
    FILE = "file"


###########################################################################
class NotebookFile:
    """Represents a Single File on the ArcGIS Notebook Server"""

    _da = None
    _definition = None

    # ---------------------------------------------------------------------
    def __init__(self, definition: dict[str, Any], da: NotebookDataAccess):
        self._definition = definition
        self._da = da

    # ---------------------------------------------------------------------
    def __str__(self):
        return f"<NotebookFile file={self.name}>"

    # ---------------------------------------------------------------------
    def __repr__(self):
        return f"NotebookFile(name={self.name})"

    # ---------------------------------------------------------------------
    @property
    def name(self) -> str:
        """
        Get the file name

        :return: str - The name of the file.
        """
        return self._definition.get("Name").strip("/").split("/")[-1]

    # ---------------------------------------------------------------------
    @property
    def properties(self):
        """
        returns the properties of the resource

        :return: Dict

        """
        return InsensitiveDict(self._definition)

    # ---------------------------------------------------------------------
    def rename(self, name: str) -> bool | dict[str, Any]:
        """
        Rename the file on the server.

        ===================  ==========================================================================
        **Parameter**         **Description**
        -------------------  --------------------------------------------------------------------------
        name                 Required String. The new name of the file.
        ===================  ==========================================================================

        :return: True if the file was renamed, False or an error as a dictionary if it was not.
        """
        return self._da._rename(
            folder_name=self._definition.get("Name"),
            new_name=name,
        )

    # ---------------------------------------------------------------------
    def download(self) -> str:
        """
        Copies down the data from the server to the local machine

        :return: str as file path
        """
        return self._da._download(filename=self._definition.get("Name"))

    # ---------------------------------------------------------------------
    @deprecated(
        deprecated_in="2.4.2",
        removed_in="2.5.0",
        details="Use the delete method instead.",
    )
    def erase(self) -> bool:
        """
        Deletes a file from the system

        :return: Boolean
        """
        return self.delete()

    # ---------------------------------------------------------------------
    def delete(self) -> bool:
        """
        Deletes a file from the system. This will permanently delete the file and the action cannot be undone.

        :return: True if the file was deleted, False or an error if it was not.
        """
        return self._da._delete(filename=self._definition.get("Name"))

    # ---------------------------------------------------------------------
    def move(self, target_folder: NotebookFolder) -> bool | dict[str, Any]:
        """
        Moves the file to another NotebookFolder.

        ===================  ============================================================
        **Parameter**         **Description**
        -------------------  ------------------------------------------------------------
        target_folder        Required NotebookFolder. The target folder to move the file to.
        ===================  ============================================================

        :return: True if the file was moved successfully, False or an error dictionary otherwise.
        """
        target_folder_path = target_folder._folder_name or ""
        if not target_folder_path.endswith("/"):
            target_folder_path += "/"

        filename = self.name
        current_path = self._definition.get("Name")
        new_path = f"{target_folder_path}{filename}"

        return self._da._rename(folder_name=current_path, new_name=new_path)

    # ---------------------------------------------------------------------
    def transfer(self, target_user: User | str | None = None) -> bool:
        """
        Transfer the file to another user in the organization. This can only be done by an administrator.
        The file will be renamed to `_transferred_{file_name}` and moved to the target user's Home folder.

        ===================  ==========================================================================
        **Parameter**         **Description**
        -------------------  --------------------------------------------------------------------------
        target_user          Required User instance or string. The user or username to which the file will be transferred.
                             If a string is provided, it should be the username of the target user.
        ===================  ==========================================================================

        :return: True if the file was transferred successfully, False or an error if it was not.
        """
        if isinstance(target_user, User):
            target_username = target_user.username
        else:
            target_username = target_user

        # Check the user has privileges to transfer folders and a workspace
        if not self._da._check_user_has_privileges(self._da._gis.users.me):
            raise ValueError(
                "Only organization administrators can transfer user folders."
            )
        if not self._da._check_user_has_workspace(target_username):
            raise ValueError(
                f"Target user '{target_username}' does not have a notebook workspace in the organization."
            )

        new_name = f"_transferred_{self.name}"
        return self._da._rename(
            folder_name=self._folder_name,
            new_name=f"{new_name}",
            username=target_username,
        )


###########################################################################
class NotebookFolder:
    """
    Represents a folder in the ArcGIS Notebook Server workspace.
    This class allows you to manage files and subfolders within a specific folder in the notebook workspace directory.
    """

    _da = None
    _folder_name = None
    _files = None

    # ---------------------------------------------------------------------
    def __init__(self, folder_name: str, da: NotebookDataAccess):
        self._folder_name = folder_name
        self._da = da
        self._url = da._url
        self._is_agol = da._gis._is_arcgisonline

    # ---------------------------------------------------------------------
    def __str__(self):
        return f"<NotebookFolder: {self.name}>"

    # ---------------------------------------------------------------------
    def __repr__(self):
        return f"NotebookFolder(name={self.name})"

    # ---------------------------------------------------------------------
    @property
    def name(self) -> str:
        """
        Get or set the name of the folder.

        ====================    ==========================================================================
        **Parameter**           **Description**
        --------------------    --------------------------------------------------------------------------
        new_name                Required String. The name of the folder. This will rename the folder.
        ====================    ==========================================================================

        :return: str - The name of the folder.
        """
        return self._folder_name.strip("/").split("/")[-1] or "Home"

    # ---------------------------------------------------------------------
    def rename(self, name: str) -> bool | dict[str, Any]:
        """
        Rename the folder.

        ===================  ==========================================================================
        **Parameter**         **Description**
        -------------------  --------------------------------------------------------------------------
        name                 Required String. The new name of the folder.
                             The name must be a simple, non-empty name without slashes.
        ===================  ==========================================================================

        :return: True if the folder was renamed, False or an error as a dictionary if it was not.
        """
        if self.name == "Home":
            raise ValueError(
                "Cannot rename the root folder 'Home'. Please create a subfolder instead."
            )
        if not isinstance(name, str):
            raise ValueError("Folder name must be a string.")
        if "/" in name or name.strip() == "":
            raise ValueError(
                "Folder name must be a simple, non-empty name without slashes."
            )
        current_name = self._folder_name
        parts = self._folder_name.strip("/").split("/")
        parts[-1] = name
        self._folder_name = "/".join(parts) + "/"

        return self._da._rename(
            folder_name=current_name,
            new_name=self._folder_name,
        )

    # ---------------------------------------------------------------------
    @property
    def folders(self) -> list[NotebookFolder]:
        """
        Returns the subfolders in the folder.

        :return: list[NotebookFolder] - A list of NotebookFolder objects representing the subfolders.
        """
        return self._da._get_folders(
            parent_folder=self._folder_name,
        )

    # ---------------------------------------------------------------------
    @property
    def files(self) -> list[NotebookFile]:
        """
        Returns the files in the folder.

        :return: list[NotebookFile] - A list of NotebookFile objects representing the files in the folder.
        """
        # Create the params dictionary for the request
        params = {
            "f": "json",
            "restype": "container",
            "comp": "list",
            "token": self._da._gis.session.auth.token,
        }

        # create urls
        if self._is_agol:
            url = f"{self._url}/{self._da._username}"
            params["delimiter"] = "/"
        else:
            url = f"{self._url}/{self._da._username}/notebookworkspace"
        if self._folder_name:
            params["prefix"] = self._folder_name
        response = self._da._gis.session.get(url, params=params).json()
        # Filter files based on ResourceType
        if self._is_agol:
            return [
                NotebookFile(f, self._da)
                for f in response.get("Blobs", [])
                if f["Properties"].get("ResourceType", "").lower() == "file"
            ]
        return [
            NotebookFile(f, self._da)
            for f in response.get("Blobs", [])
            if not f["Name"].endswith("/")
        ]

    # ---------------------------------------------------------------------
    def create_folder(self, folder_name: str) -> NotebookFolder:
        """
        Create a subfolder in the current folder.

        ===================  ==========================================================================
        **Parameter**         **Description**
        -------------------  --------------------------------------------------------------------------
        folder_name          Required String. The name of the subfolder to create.
                             This will create a subfolder within the current folder.
        ===================  ==========================================================================

        :return: NotebookFolder - A NotebookFolder object representing the newly created subfolder.
        """
        if self._is_agol:
            url = f"{self._url}/{self._da._username}/createFolder".replace(
                "/azureblob", ""
            )
        else:
            url = f"{self._url}/notebookworkspace/createFolder"
        params = {
            "f": "json",
            "folderName": f"{self._folder_name}{folder_name}",
            "token": self._da._gis.session.auth.token,
        }
        result = self._da._gis.session.post(url, data=params).json()
        if result.get("status") == "success":
            return NotebookFolder(f"{self._folder_name}{folder_name}/", self._da)

    # ---------------------------------------------------------------------
    def _resolve_files(self, fp):
        if isinstance(fp, list):
            return [f for f in fp if os.path.isfile(f)]

        if os.path.isdir(fp):
            # If the path is a directory, get all files in the directory
            return [
                os.path.join(fp, f)
                for f in os.listdir(fp)
                if os.path.isfile(os.path.join(fp, f))
            ]

        if os.path.isfile(fp):
            # If the path is a file, return it as a list
            return [fp]

        raise ValueError(
            f"Invalid file path: {fp}. It must be a file or a directory containing files."
        )

    # ---------------------------------------------------------------------
    def _upload_single_file(self, file_path: str) -> bool:
        if not os.path.isfile(file_path):
            raise ValueError(f"File {file_path} does not exist.")

        filename = os.path.basename(file_path)

        if self._is_agol:
            if any(f.name == filename for f in self.files):
                raise ValueError(f"File {filename} already exists in the workspace.")

        # Add folder path unless root folder
        full_path = (
            f"{self._folder_name}{filename}" if self._folder_name != "/" else filename
        )

        if self._is_agol:
            url = f"{self._url}/{self._da._username}/{full_path}"
        else:
            url = f"{self._url}/notebookworkspace/{full_path}"

        original_headers = copy.deepcopy(self._da._gis.session.headers)
        token_header = "X-Esri-Authorization"
        if (
            self._da._gis._session.auth.token
            and "X-Esri-Authorization" not in original_headers
        ):
            token = self._da._gis._session.auth.token
        # Needed for online and enterprise
        original_headers.update(
            {token_header: "Bearer %s" % token, "x-ms-blob-type": "BlockBlob"}
        )
        with open(file_path, "rb") as file_data:
            resp = self._da._gis.session.put(
                url=url,
                data=file_data,
                verify=True,
                headers=original_headers,
            )
        return 200 <= resp.status_code < 300

    # ---------------------------------------------------------------------
    def upload(self, fp: str | list[str]) -> list[bool]:
        """
        Uploads a file to the Notebook Server in the current folder.

        ===================     ==========================================================================
        **Parameter**           **Description**
        -------------------     --------------------------------------------------------------------------
        fp                      Required String or list of Strings. Either: the path to the file to upload,
                                a list of paths to the files to upload, or the path to a folder where all
                                the files in the folder will be uploaded under the folder name.
        ===================     ==========================================================================

        :return: list of booleans. True if the file was uploaded, False or an error if it was not.
        """
        # Get files as a list
        files = self._resolve_files(fp)

        if not files:
            raise ValueError(
                "No valid files found to upload. Please provide a valid file path or directory."
            )

        responses = []
        for file in files:
            responses.append(self._upload_single_file(file))
        return responses

    # ---------------------------------------------------------------------
    def move(self, target_folder: NotebookFolder) -> bool:
        """
        Move the folder to another folder in the notebook workspace.

        ===================  ==========================================================================
        **Parameter**         **Description**
        -------------------  --------------------------------------------------------------------------
        target_folder        Required NotebookFolder. The target folder to which this folder will be moved.
                             The target folder must be a valid NotebookFolder object.
        ===================  ==========================================================================

        :return: True if the folder was moved successfully, False or an error if it was not.
        """
        if not isinstance(target_folder, NotebookFolder):
            raise ValueError("target_folder must be a NotebookFolder instance.")

        if self.name == "Home":
            raise ValueError(
                "Cannot move the root folder 'Home'. Please create a subfolder instead."
            )

        target_path = target_folder._folder_name or ""
        if not target_path.endswith("/") and target_path != "":
            target_path += "/"

        parts = self._folder_name.strip("/").split("/")
        new_name = f"{target_path}{parts[-1]}/"

        return self._da._rename(folder_name=self._folder_name, new_name=new_name)

    # ---------------------------------------------------------------------
    def transfer(self, target_user: User | str | None = None) -> bool | dict[str, Any]:
        """
        Transfer the folder to another user in the organization. This can only be done by an administrator.
        The folder will be renamed to `_transferred_{folder_name}` and moved to the target user's Home folder.

        ===================  ==========================================================================
        **Parameter**         **Description**
        -------------------  --------------------------------------------------------------------------
        target_user          Required User instance or string. The user or username to which the folder will be transferred.
                             If a string is provided, it should be the username of the target user.
        ===================  ==========================================================================

        :return: True if the folder was transferred successfully. If not then, False or an error dictionary.
        """
        if isinstance(target_user, User):
            target_username = target_user.username
        else:
            target_username = target_user

        # Check the user has privileges to transfer folders and a workspace
        if not self._da._check_user_has_privileges(self._da._gis.users.me):
            raise ValueError(
                "Only organization administrators can transfer user folders."
            )
        if not self._da._check_user_has_workspace(target_username):
            raise ValueError(
                f"Target user '{target_username}' does not have a notebook workspace in the organization."
            )

        new_name = f"_transferred_{self._folder_name.strip('/').split('/')[-1]}"
        return self._da._rename(
            folder_name=self._folder_name,
            new_name=f"{new_name}",
            username=target_username,
        )

    # ---------------------------------------------------------------------
    def delete(self) -> bool:
        """
        Deletes a folder and all content from the system. This will permanently delete the folder and content and the action cannot be undone.

        :return: True if the folder and content was deleted, False or an error if it was not.
        """
        if self.name == "Home":
            raise ValueError("Cannot delete the root folder 'Home'.")
        return self._da._delete(filename=self._folder_name)


###########################################################################
class NotebookDataAccess:
    """
    The Data Access Workspace Directory allows notebook authors to manage files used in their notebooks.
    """

    _url = None
    _gis = None

    # ---------------------------------------------------------------------
    def __init__(self, url, gis, username: str | None = None):
        self._url = url
        self._gis = gis
        self._username = username or gis.users.me.username
        if not self._check_user_has_workspace(self._username):
            raise ValueError(
                f"User {self._username} does not have a notebook workspace in the organization."
            )

    # --------------------------------------------------------------------
    def __repr__(self):
        return f"NotebookDataAccess(username={self._username})"

    # ---------------------------------------------------------------------
    def __str__(self):
        return "Notebook Workspace for: " + self._username

    # ---------------------------------------------------------------------
    def _get_folders(self, parent_folder: str | None) -> list[NotebookFolder]:
        params = {
            "f": "json",
            "restype": "container",
            "comp": "list",
            "token": self._gis.session.auth.token,
        }
        if self._gis._is_agol:
            url = f"{self._url}/{self._username}"
            params["delimiter"] = "/"
        else:
            url = f"{self._url}/{self._username}/notebookworkspace"
        if parent_folder:
            params["prefix"] = parent_folder
        response = self._gis.session.get(url, params=params).json()
        # When creating subfolders the name should always have the folder to which it belongs as the prefix

        if self._gis._is_agol:
            folders = [
                NotebookFolder(f["Name"], self)
                for f in response.get("Blobs", [])
                if f["Properties"].get("ResourceType", "").lower() == "directory"
            ]
        else:
            folders = [
                NotebookFolder(f["Name"], self)
                for f in response.get("Blobs", [])
                if f["Name"].endswith("/") and f["Name"] != parent_folder
            ]

        # Include root folder only if folder_name is None
        if parent_folder is None:
            folders.insert(0, NotebookFolder("", self))
        return folders

    # ---------------------------------------------------------------------
    def get_workspace(self, user: User | str) -> NotebookDataAccess:
        """
        Returns the NotebookDataAccess object for the specified user. This is only available to organization administrators.

        ====================    ==========================================================================
        **Parameter**           **Description**
        --------------------    --------------------------------------------------------------------------
        user                    Required User instance or string. The user or username for which the workspace will be retrieved.
        ====================    ==========================================================================

        :return: NotebookDataAccess - A NotebookDataAccess object for the specified user.
        """
        # Check if the user is an administrator
        if not self._check_user_has_privileges(self._gis.users.me):
            raise ValueError(
                "Only organization administrators can access other users' workspaces."
            )
        if isinstance(user, str):
            user = self._gis.users.get(user)
            if not user:
                raise ValueError(f"User '{user}' not found in the organization.")
        # Check if the user has a workspace
        if not self._check_user_has_workspace(user.username):
            raise ValueError(
                f"User '{user.username}' does not have a notebook workspace in the organization."
            )
        return NotebookDataAccess(self._url, self._gis, username=user.username)

    # ---------------------------------------------------------------------
    @property
    def folders(self) -> list[NotebookFolder]:
        """
        Returns the folders in the workspace directory (/arcgis/home) of the user making the request.

        :return: list[NotebookFolder] - A list of NotebookFolder objects containing the folders in the workspace.
        """
        return self._get_folders(
            parent_folder=None,
        )

    # ---------------------------------------------------------------------
    def _get_folder(self, folder_name: str) -> NotebookFolder:
        """
        Returns a specific folder in the workspace directory (/arcgis/home) of the user making the request.
        If you have multiple folders with the same name, this method will return the first one found.

        ====================    ==========================================================================
        **Parameter**           **Description**
        --------------------    --------------------------------------------------------------------------
        folder_name             Required String. The name of the folder to retrieve.
                                The folder name must be a simple, non-empty name without slashes.
        ====================    ==========================================================================

        :return: NotebookFolder - A NotebookFolder object representing the requested folder.
        """
        if not isinstance(folder_name, str):
            raise ValueError("folder_name must be a string.")
        if not folder_name:
            raise ValueError("folder_name cannot be empty.")

        # Recursively search for the folder
        def _find_folder(parent_folder):
            folders = self._get_folders(parent_folder)
            for folder in folders:
                if folder.name == folder_name:
                    return folder
                # Recursively search subfolders
                for folder in folders:
                    if folder.name != "Home":  # Avoid infinite loop on root
                        found = _find_folder(folder._folder_name)
                        if found:
                            return found
            return None

        result = _find_folder(None)
        if result is None:
            raise ValueError(
                f"Folder '{folder_name}' not found in the workspace. If you meant to get a file please set type to DATAACCESSTYPE.FILE"
            )
        return result

    # ---------------------------------------------------------------------
    def _get_file(self, file_name: str) -> NotebookFile | None:
        """
        Returns a specific file in the workspace directory (/arcgis/home) of the user making the request.
        If you have multiple files with the same name, this method will return the first one found.

        ====================    ==========================================================================
        **Parameter**           **Description**
        --------------------    --------------------------------------------------------------------------
        file_name               Required String. The name of the file to retrieve.
                                The file name must be a simple, non-empty name without slashes.
        ====================    ==========================================================================

        :return: NotebookFile - A NotebookFile object representing the requested file, or None if not found.
        """
        if not isinstance(file_name, str):
            raise ValueError("file_name must be a string.")
        if not file_name:
            raise ValueError("file_name cannot be empty.")

        if self._gis._is_agol:
            url = f"{self._url}/notebooksWorkspace"
        else:
            url = f"{self._url}/notebookworkspace"
        params = {
            "f": "json",
            "restype": "container",
            "comp": "list",
            "token": self._gis.session.auth.token,
        }
        try:
            response = self._gis.session.get(url, params=params).json()
        except Exception as ex:
            raise RuntimeError(f"Failed to fetch files: {ex}")

        for f in response.get("Blobs", []):
            if self._gis._is_agol:
                if f.get("Properties", {}).get(
                    "ResourceType", ""
                ).lower() == "file" and f["Name"].endswith(file_name):
                    return NotebookFile(f, self)
            else:
                if f["Name"].endswith(file_name) and not f["Name"].endswith("/"):
                    return NotebookFile(f, self)
        return None

    # ---------------------------------------------------------------------
    def get(
        self, name: str, type: DATAACCESSTYPE | str = DATAACCESSTYPE.FOLDER
    ) -> NotebookFolder | NotebookFile:
        """
        Get a notebook folder or file by name.

        ====================    ==========================================================================
        **Parameter**           **Description**
        --------------------    --------------------------------------------------------------------------
        name                    Required String. The name of the folder or file to retrieve.
                                If type is DATAACCESSTYPE.FOLDER, it retrieves a folder; otherwise, it retrieves a file.
        ---------------------    --------------------------------------------------------------------------
        type                    Optional DATAACCESSTYPE. If DATAACCESSTYPE.FOLDER, retrieves a folder; if DATAACCESSTYPE.FILE, retrieves a file.
                                Default is DATAACCESSTYPE.FOLDER.
        ====================    ==========================================================================

        :return: NotebookFolder or NotebookFile - The requested folder or file.
        """
        if type == DATAACCESSTYPE.FOLDER or (
            isinstance(type, str) and type.lower() == "folder"
        ):
            return self._get_folder(name)
        else:
            return self._get_file(name)

    # ---------------------------------------------------------------------
    def _check_user_has_workspace(self, username: str) -> bool:
        """
        Checks if the user has a workspace in the organization.

        :param user: User instance or username to check.
        :return: True if the user has a workspace, False otherwise.
        """
        workspace_url = f"{self._url}/listUserWorkspaces".replace("/azureblob/", "/")

        try:
            res = self._gis.session.get(workspace_url, params={"f": "json"}).json()
        except Exception as ex:
            raise RuntimeError(f"Failed to fetch workspaces: {ex}")

        all_workspaces = res.get("Containers", [])
        return any(workspace.get("Name") == username for workspace in all_workspaces)

    # ---------------------------------------------------------------------
    def _check_user_has_privileges(self, user: User) -> bool:
        privileges = user.privileges or []
        return (
            user.role == "org_admin"
            if self._gis._is_agol
            else "portal:admin:manageServers" in privileges
            and "portal:admin:manageSecurity" in privileges
        )

    # ---------------------------------------------------------------------
    def transfer(
        self,
        source_user: User | str,
        target_user: User | str | None = None,
        folder_name: str | None = None,
    ) -> bool:
        """
        Transfer the workspace of one user to another user in the organization.
        This can only be done by an administrator.

        This method is useful for transferring the workspace of a user who is leaving the organization to another user.

        ===================  ==========================================================================
        **Parameter**        **Description**
        -------------------  --------------------------------------------------------------------------
        source_user          Required User instance or string. The user or username for which the workspace will be transferred.
        -------------------  --------------------------------------------------------------------------
        target_user          Optional User instance or string. The user or username to which the workspace will be transferred.
                             If not provided, the workspace will be transferred to the current user.
        -------------------  --------------------------------------------------------------------------
        folder_name          Optional String. The name of the folder to which the workspace will be transferred.
                             If not provided, a default name will be used.
        ===================  ==========================================================================

        :return: True if the transfer was successful, False or an error if it was not.
        """
        ### Check if the user is an administrator and can transfer workspaces
        if not self._check_user_has_privileges(self._gis.users.me):
            raise ValueError(
                "Only organization administrators can transfer user workspaces."
            )

        # Resolve source_user
        if isinstance(source_user, User):
            source_username = source_user.username
        else:
            source_username = source_user
            source_user = self._gis.users.get(source_username)

        # Resolve target_user
        if target_user is None:
            target_user = self._gis.users.me
            target_username = target_user.username
        elif isinstance(target_user, User):
            target_username = target_user.username
        else:
            target_username = target_user
            target_user = self._gis.users.get(target_username)

        # Ensure users exist
        if not source_user or not target_user:
            raise ValueError(
                f"Source user '{source_username}' or target user '{target_username}' does not exist in the organization."
            )

        # Check both users have workspaces
        if not self._check_user_has_workspace(source_username):
            raise ValueError(
                f"Source user '{source_username}' does not have a notebook workspace in the organization."
            )
        if not self._check_user_has_workspace(target_username):
            raise ValueError(
                f"Target user '{target_username}' does not have a workspace in the organization."
            )

        # if folder_name is None, create the default folder name
        if folder_name is None:
            folder_name = f"_transferred_{source_username}"

        url = f"{self._url}/transferUserWorkspace".replace("/azureblob/", "/")
        params = {
            "f": "json",
            "targetFoldername": folder_name,
            "userName": source_username,
        }
        if self._gis._is_arcgisonline:
            params["targetUserName"] = target_username
        else:
            params["targetUsername"] = target_username
        try:
            res = self._gis.session.post(url, params).json()
        except Exception as ex:
            raise RuntimeError(f"Workspace transfer request failed: {ex}")

        if res.get("status") == "success":
            return True
        elif "error" in res:
            raise ValueError(
                f"Error transferring user workspace: {res['error']['message']}"
            )
        else:
            raise ValueError(f"Unknown error during workspace transfer: {res}")

    def _extract_filename(self, content_disposition_string: str) -> str | None:
        """
        Extracts the filename from a Content-Disposition header string.
        Handles both quoted and unquoted filenames, and prioritizes filename* for UTF-8.
        """

        if not content_disposition_string:
            return None

        # Try to extract filename* (for UTF-8 encoded filenames) first
        match_utf8 = re.search(
            r"filename\*=UTF-8''([^;]+)", content_disposition_string, re.IGNORECASE
        )
        if match_utf8:
            # Decode URL-encoded characters
            import urllib.parse

            return urllib.parse.unquote(match_utf8.group(1))

        # Then try to extract quoted filename
        match_quoted = re.search(
            r'filename="([^"]+)"', content_disposition_string, re.IGNORECASE
        )
        if match_quoted:
            return match_quoted.group(1)

        # Finally, try to extract unquoted filename
        match_unquoted = re.search(
            r"filename=([^;]+)", content_disposition_string, re.IGNORECASE
        )
        if match_unquoted:
            return match_unquoted.group(1).strip()

        return None

    def _is_file(self, response: requests.Response) -> bool:
        """checks if the response contains a file"""
        content_type = response.headers.get("Content-Type")
        content_disposition = response.headers.get("Content-Disposition")
        file_name: str | None = self._extract_filename(content_disposition)
        is_file = False, file_name

        if content_disposition and "attachment" in content_disposition:
            is_file = True, file_name
        elif (
            content_type
            and "text/html" not in content_type
            and "application/json" not in content_type
        ):
            is_file = True, file_name
        return is_file

    # ---------------------------------------------------------------------
    def _download(self, filename: str) -> str:
        """
        downloads a file from the notebook server
        """

        if self._gis._is_arcgisonline:
            url = f"{self._url.replace('/azureblob', '')}/{self._gis.users.me.username}/downloadFile"
            params = {
                "fileName": filename,
            }
            response: requests.Response = self._gis.session.get(url, params=params)
            is_file, file_name = self._is_file(response)
            if is_file:

                folder: str = tempfile.gettempdir()
                fp = os.path.join(folder, file_name)
                with open(fp, "wb") as writer:
                    writer.write(response.content)
                return fp
        else:
            url = f"{self._url}/notebookworkspace/downloadFile"
            params = {
                "f": "json",
                "fileName": filename,
            }
            response: requests.Response = self._gis.session.post(url, data=params)
            is_file, file_name = self._is_file(response)
            if is_file:

                folder: str = tempfile.gettempdir()
                fp = os.path.join(folder, file_name)
                with open(fp, "wb") as writer:
                    writer.write(response.content)
                return fp
        return self._gis.session.post(url, data=params).json()

    # ---------------------------------------------------------------------
    def _delete(self, filename: str) -> bool:
        """
        downloads a file from the
        """
        if self._gis._is_arcgisonline:
            url = f"{self._url}/deleteFile".replace("/azureblob", f"/{self._username}")
        else:
            url = f"{self._url}/notebookworkspace/deleteFile"
        params = {
            "f": "json",
            "fileName": filename,
        }
        return (
            self._gis.session.post(url, data=params).json().get("status") == "success"
        )

    # ---------------------------------------------------------------------
    def _rename(
        self, folder_name: str, new_name: str, username: str | None = None
    ) -> bool | dict[str, Any]:
        """
        Renames a folder in the notebook workspace.

        :param folder_name: The current name of the folder to rename.
        :param new_name: The new name for the folder.
        :return: True if the folder was renamed successfully, False otherwise.
        """
        params = {
            "f": "json",
            "source": folder_name,
            "target": new_name,
            "token": self._gis.session.auth.token,
        }
        if self._gis._is_agol:
            url = f"{self._url}/move".replace("/azureblob/", f"/{self._username}/")
            params["targetUserName"] = username or self._username
        else:
            url = f"{self._url}/{self._username}/notebookworkspace/move"
            params["targetUsername"] = username or self._username

        resp = self._gis.session.post(url, params).json()
        if resp.get("status") == "success":
            return True
        return resp["error"]

    # ---------------------------------------------------------------------
    @property
    @deprecated(
        deprecated_in="2.4.2",
        removed_in="2.5.0",
        details="Use the files property found in a NotebookFolder instead or the get method with DATAACCESSTYPE.FILE.",
    )
    def files(self) -> list[NotebookFile]:
        """
        lists files that are located in the workspace directory (/arcgis/home) of the user making the request.

        :return: list[NotebookFile] - list of NotebookFile objects
        """
        params = {
            "f": "json",
            "restype": "container",
            "comp": "list",
            "token": self._gis.session.auth.token,
        }

        # create urls
        if self._gis._is_agol:
            url = f"{self._url}/{self._username}"
            params["delimiter"] = "/"
        else:
            url = f"{self._url}/{self._username}/notebookworkspace"
        response = self._gis.session.get(url, params=params).json()
        # Filter files based on ResourceType
        if self._gis._is_agol:
            return [
                NotebookFile(f, self)
                for f in response.get("Blobs", [])
                if f["Properties"].get("ResourceType", "").lower() == "file"
            ]
        return [NotebookFile(f, self) for f in response.get("Blobs", [])]

    # ---------------------------------------------------------------------
    @deprecated(
        deprecated_in="2.4.2",
        removed_in="2.5.0",
        details="Use the create_folder method found in a NotebookFolder instead. The first folder in the list of folders is the Home folder.",
    )
    def create_folder(self, folder: str) -> bool:
        """
        Create a folder in your `/arcgis/home` notebook workspace directory.

        ===================  ==========================================================================
        **Parameter**         **Description**
        -------------------  --------------------------------------------------------------------------
        folder               Required String. The name of the folder to create. To create a folder in a subfolder,
                             use the format `subfolder1/subfolder2/foldername`. If you want to create a folder
                             in the root directory, use the format `foldername`.
        ===================  ==========================================================================

        """
        if self._gis._is_agol:
            url = f"{self._url}/{self._username}/createFolder".replace("/azureblob", "")
        else:
            url = f"{self._url}/notebookworkspace/createFolder"
        params = {
            "f": "json",
            "folderName": folder,
            "token": self._gis.session.auth.token,
        }
        return self._gis.session.post(url, params).json().get("status") == "success"

    # ---------------------------------------------------------------------
    @deprecated(
        deprecated_in="2.4.2",
        removed_in="2.5.0",
        details="Use the upload method found in a NotebookFolder instead.",
    )
    def upload(self, fp: str | list[str], folder: str | None = None) -> list[bool]:
        """
        Uploads a file to the Notebook Server

        ===================     ==========================================================================
        **Parameter**           **Description**
        -------------------     --------------------------------------------------------------------------
        fp                      Required String or list of Strings. Either: the path to the file to upload,
                                a list of paths to the files to upload, or the path to a folder where all
                                the files in the folder will be uploaded under the folder name.
        -------------------     --------------------------------------------------------------------------
        folder                  Optional String. The name of the folder to upload the file to. If not provided,
                                the file will be uploaded to the root directory of the notebook workspace.
                                Example: `folder1`
        ===================     ==========================================================================

        :return: list of booleans. True if the file was uploaded, False or an error if it was not.
        """
        folder = self._get_folder(folder) if folder else self.folders[0]
        return folder.upload(fp)
