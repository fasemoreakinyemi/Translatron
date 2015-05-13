#!/usr/bin/env python3
"""
Downloads assets to the static directory.
This is used to allow easier updating without having to clog the git repository
with frequently updated minified JS/CSS
"""
import urllib.request
import os
import sys
from ansicolor import black, red
from collections import namedtuple

Asset = namedtuple("Asset", ["filename", "url"])
assets = [
    #Bootstrap
    Asset("css/bootstrap.min.css", "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/css/bootstrap.min.css"),
    Asset("js/bootstrap.min.js", "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/js/bootstrap.min.js"),
    Asset("fonts/glyphicons-halflings-regular.eot", "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/fonts/glyphicons-halflings-regular.eot"),
    Asset("fonts/glyphicons-halflings-regular.svg", "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/fonts/glyphicons-halflings-regular.svg"),
    Asset("fonts/glyphicons-halflings-regular.ttf", "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/fonts/glyphicons-halflings-regular.ttf"),
    Asset("fonts/glyphicons-halflings-regular.woff", "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/fonts/glyphicons-halflings-regular.woff"),
    Asset("fonts/glyphicons-halflings-regular.woff2", "https://maxcdn.bootstrapcdn.com/bootstrap/3.3.2/fonts/glyphicons-halflings-regular.woff2"),
    #Angular
    Asset("js/angular.min.js", "https://ajax.googleapis.com/ajax/libs/angularjs/1.3.13/angular.min.js"),
    #Angular Bootstrap directives
    Asset("js/angular-bootstrap.min.js", "https://angular-ui.github.io/bootstrap/ui-bootstrap-tpls-0.12.0.min.js"),
    #JQuery & plugins
    Asset("js/jquery.min.js", "https://ajax.googleapis.com/ajax/libs/jquery/2.1.3/jquery.min.js"),
    Asset("js/jquery.highlight.js", "https://raw.githubusercontent.com/bartaz/sandbox.js/master/jquery.highlight.js"),
]

def ensureFileIsPresent(asset, directory, forceDownload=False):
    (filename, url) = asset
    filepath = os.path.join(directory, filename)
    if url is None: # --> no need to download
        return
    if not os.path.isfile(filepath) or forceDownload:
        print (black("Downloading %s" % filename, bold=True))
        urllib.request.urlretrieve(url, filepath)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--force-download", action="store_true", help="Force downloading assets")
    parser.add_argument("-d", "--directory", default="./static", help="Directory to download (must exist)")
    args = parser.parse_args()
    #
    if not os.path.isdir(args.directory):
        print(red("%s is not a directory" % args.directory, bold=True))
        sys.exit(1)
    #Run download if file
    for asset in assets:
        ensureFileIsPresent(asset, args.directory, args.force_download)