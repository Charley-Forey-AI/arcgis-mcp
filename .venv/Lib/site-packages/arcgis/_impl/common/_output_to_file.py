from __future__ import annotations
import os
import requests
from requests_toolbelt.downloadutils import stream


def handle_response(
    resp: requests.Response,
    file_name: str,
    out_path: str,
    try_json: bool,
    force_bytes: bool = False,
    ignore_error_key: bool = False,
) -> dict:
    """
    handles the request responses

    ===========================   =====================================================
    **optional keywords**         **description**
    ---------------------------   -----------------------------------------------------
    resp                          required requests.Response object.
    ---------------------------   -----------------------------------------------------
    file_name                     required string.  Name of the output file if needed.
    ---------------------------   -----------------------------------------------------
    out_path                      required string. Name of the save folder.
    ---------------------------   -----------------------------------------------------
    try_json                      required boolean. Determines if the response should
                                  be returned as a dictionary or the native format
                                  send back by a server
    ---------------------------   -----------------------------------------------------
    force_bytes                   Optional Boolean. If True, the results returns as bytes
                                  instead of a file path.
    ===========================   =====================================================

    returns: string, dictionary, or bytes depending on the response.

    """
    data = None
    url = resp.url
    if out_path and os.path.isdir(out_path) == False:
        os.makedirs(out_path)
    if out_path is None:
        out_path = tempfile.gettempdir()
    if (
        file_name is None
        and "Content-Type" in resp.headers
        and (
            resp.headers["Content-Type"].lower().find("json") == -1
            and resp.headers["Content-Type"].lower().find("text") == -1
        )
    ):
        file_name = (
            _filename_from_url(url) or _filename_from_headers(resp.headers) or None
        )
    elif file_name is None and "Content-Disposition" in resp.headers:
        file_name = (
            _filename_from_url(url) or _filename_from_headers(resp.headers) or None
        )
    if force_bytes:
        try:
            return bytes(resp.content)
        except:
            return resp.content
    if file_name is not None:
        file_name = os.path.join(out_path, file_name)
        if os.path.isfile(file_name):
            os.remove(file_name)
        stream_size = 512 * 2
        if "Content-Length" in resp.headers:
            max_length = int(resp.headers["Content-Length"])
            if max_length > stream_size * 2 and max_length < 1024 * 1024:
                stream_size = 1024 * 2
            elif max_length > 5 * (1024 * 1024) and max_length < 10 * (1024 * 1024):
                stream_size = 5 * (1024 * 1024)  # 5 mb
            elif max_length >= 10 * (1024 * 1024):
                stream_size = 10 * (1024 * 1024)  # 10 mb
            elif max_length > (1024 * 1024):
                stream_size = 1024 * 1024  # 1 mb
            else:
                stream_size = 512 * 2

        fp = stream.stream_response_to_file(
            response=resp, path=file_name, chunksize=stream_size
        )
        return fp

    if try_json:
        if (
            "Content-Length" in resp.headers
            and int(resp.headers.get("Content-Length")) == 0
        ):
            data = {}
        elif (
            "Transfer-Encoding" in resp.headers
            and resp.headers["Transfer-Encoding"].lower() == "chunked"
        ):
            data = None
            for it in resp.iter_lines(
                chunk_size=None, decode_unicode=True, delimiter=None
            ):
                if data is None:
                    data = it
                else:
                    data += it
            data = json.loads(data)
            if "error" in data and ignore_error_key == False:
                raise Exception(data["error"])
        else:
            try:
                data = resp.json()
            except JSONDecodeError:
                if resp.text:
                    raise Exception(resp.text)
                else:
                    raise
        if "error" in data and ignore_error_key == False:
            if "messages" in data:
                return data
            errorcode = data["error"]["code"] if "code" in data["error"] else 0
            handle_json_error(data["error"], errorcode)
        return data
    else:
        return resp.text


# ----------------------------------------------------------------------
def handle_json_error(error, errorcode):
    errormessage = error.get("message")
    # handles case where message exists in the dictionary but is None
    if errormessage is None:
        errormessage = "Unknown Error"
    # _log.error(errormessage)
    if "details" in error and error["details"] is not None:
        if isinstance(error["details"], str):
            errormessage = f"{errormessage} \n {error['details']}"
            # _log.error(error['details'])
        else:
            for errordetail in error["details"]:
                if isinstance(errordetail, str):
                    errormessage = errormessage + "\n" + errordetail
                    # _log.error(errordetail)

    errormessage = errormessage + "\n(Error Code: " + str(errorcode) + ")"
    raise Exception(errormessage)
