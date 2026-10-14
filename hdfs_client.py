"""Minimal WebHDFS client (stdlib only)."""
import json
import socket
import urllib.error
import urllib.parse
import urllib.request

import config


def _url(path, op, **params):
    params.update({"op": op, "user.name": config.HDFS_USER})
    return "%s/webhdfs/v1%s?%s" % (config.WEBHDFS_URL, urllib.parse.quote(path), urllib.parse.urlencode(params))


def _local(url):
    """DataNode URLs come back with this machine's hostname, which Windows can take
    seconds to resolve. On a single-node cluster, talk to it on the NameNode's address."""
    parts = urllib.parse.urlsplit(url)
    if parts.hostname and parts.hostname.lower() == socket.gethostname().lower():
        host = urllib.parse.urlsplit(config.WEBHDFS_URL).hostname
        parts = parts._replace(netloc="%s:%d" % (host, parts.port))
    return urllib.parse.urlunsplit(parts)


def _call(method, url, data=None, timeout=30):
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/octet-stream")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def mkdirs(path):
    _call("PUT", _url(path, "MKDIRS"))


def create(path, data, overwrite=False):
    """Write bytes to a new file. The NameNode hands back the DataNode URL to upload to."""
    body = _call("PUT", _url(path, "CREATE", overwrite=str(overwrite).lower(), noredirect="true"))
    _call("PUT", _local(json.loads(body)["Location"]), data=data)


def rename(src, dst):
    return json.loads(_call("PUT", _url(src, "RENAME", destination=dst)))["boolean"]


def read(path):
    body = _call("GET", _url(path, "OPEN", noredirect="true"))
    return _call("GET", _local(json.loads(body)["Location"]))


def listdir(path):
    """Return the FileStatus dicts under path, or [] if it does not exist."""
    try:
        return json.loads(_call("GET", _url(path, "LISTSTATUS")))["FileStatuses"]["FileStatus"]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return []
        raise


def delete(path, recursive=False):
    _call("DELETE", _url(path, "DELETE", recursive=str(recursive).lower()))
