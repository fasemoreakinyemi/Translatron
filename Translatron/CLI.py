#!/usr/bin/env python3
# -*- coding: utf8 -*-
import argparse
import sys
import code
import re
import os
import functools
from ansicolor import blue, red, black, green
from multiprocessing import cpu_count
from YakDB.Dump import dumpYDF, importYDFDump
import YakDB


def checkConnection(args):
    import YakDB
    #Check request/reply connection
    print (blue("Checking request/reply connection...", bold=True))
    conn = YakDB.Connection()
    conn.connect(args.req_endpoint)
    #Request server info
    print((conn.serverInfo()).decode("utf-8"))
    print(green("REQ/REP connection attempt successful"))
    #Check push/pull connection
    print (blue("Checking push/pull connection...", bold=True))
    print(green("PUSH/PULL connection attempt successful"))
    conn = YakDB.Connection()
    conn.usePushMode()
    conn.connect(args.push_endpoint)


def index(args):
    from Translatron.Indexing.NLTKIndexer import runIndexerCLITool
    runIndexerCLITool(args)


def importDocuments(args):
    from Translatron.DocumentImport.PMC import runPMCImporterCLITool
    runPMCImporterCLITool(args)


def importEntities(args):
    for infile in args.infile:
        basename = os.path.basename(infile)
        if re.match(r"uniprot_[a-z]+\.dat\.gz", basename):
            print(blue("Importing UniProt file..."))
            from Translatron.Entities.UniProtImporter import importUniprot
            importUniprot(args, infile)
        elif re.match(r"d\d{4}.bin", basename):
            print(blue("Importing MeSH file..."))
            from Translatron.Entities.MeSHImporter import importMeSH
            importMeSH(args, infile)
        elif re.match(r"[a-z][a-z]wiki.+titles.+\.gz", basename):
            print(blue("Importing Wikipedia page title file..."))
            from Translatron.Entities.WikipediaImporter import importWikimediaPagelist
            importWikimediaPagelist(args, infile)
        else:
            print (red("Can't interpret entity input file (uniprot_sprot.dat.gz - UniProt) %s " % basename))


def runServer(args):
    "Run the main translatron server. Does not terminate."
    from Translatron.Server import startTranslatron
    startTranslatron(http_port=args.http_port)


def repl(dbargs):
    code.InteractiveConsole(locals={}).interact("Translatron REPL (prototype)")

def __getDumpFilenames(args):
    "Generate a tuple of filenames to dump to / restore from"
    suffix = ".xz" if hasattr(args, "xz") and args.xz else ".gz"
    prefix = args.outprefix if hasattr(args, "outprefix") else args.inprefix
    documentsFilename = prefix  + ".documents.ydf" + suffix
    entitiesFilename = prefix  + ".entities.ydf" + suffix
    docidxFilename = prefix  + ".docidx.ydf" + suffix
    entityidxidxFilename = prefix  + ".entityidx.ydf" + suffix
    return (documentsFilename, entitiesFilename, docidxFilename, entityidxidxFilename)

def exportDump(args):
    #Setup raw YakDB connection
    conn = YakDB.Connection()
    conn.connect(args.req_endpoint)
    #Filenames to dump to
    filenames = __getDumpFilenames(args)
    #Dump every table
    if not args.no_documents:
        print (blue("Dumping document table to " + filenames[0], bold=True))
        dumpYDF(conn, filenames[0], 1)
    if not args.no_entities:
        print (blue("Dumping entity table to " + filenames[1], bold=True))
        dumpYDF(conn, filenames[1], 2)
    if not args.no_document_idx:
        print (blue("Dumping document index table to " + filenames[2], bold=True))
        dumpYDF(conn, filenames[2], 3)
    if not args.no_entity_idx:
        print (blue("Dumping entity index table to " + filenames[3], bold=True))
        dumpYDF(conn, filenames[3], 4)

def restoreDump(args):
    #Setup raw YakDB connection
    conn = YakDB.Connection()
    conn.connect(args.req_endpoint)
    #Filenames to dump to
    filenames = __getDumpFilenames(args)
    #NOTE: Partial & incremental restore is supported
    #Restory every table if the corresponding file exists
    if not args.no_documents:
        if not os.path.isfile(filenames[0]):
            print (red("Can't find document table file " + filenames[0], bold=True))
        else: #It's a regular file
            print (blue("Restoring document table from " + filenames[0], bold=True))
            importYDFDump(conn, filenames[0], 1)
    if not args.no_entities:
        if not os.path.isfile(filenames[1]):
            print (red("Can't find entity table file " + filenames[1], bold=True))
        else: #It's a regular file
            print (blue("Restoring entity table from " + filenames[1], bold=True))
            importYDFDump(conn, filenames[1], 2)
    if not args.no_document_idx:
        if not os.path.isfile(filenames[2]):
            print (red("Can't find document index table file " + filenames[2], bold=True))
        else: #It's a regular file
            print (blue("Restoring document index table from " + filenames[2], bold=True))
            importYDFDump(conn, filenames[2], 3)
    if not args.no_entity_idx:
        if not os.path.isfile(filenames[3]):
            print (red("Can't find document index table file " + filenames[3], bold=True))
        else: #It's a regular file
            print (blue("Restoring entity index table from " + filenames[3], bold=True))
            importYDFDump(conn, filenames[3], 4)

def compact(args):
    "Compact one ore more table"
    #Setup raw YakDB connection
    conn = YakDB.Connection()
    conn.connect(args.req_endpoint)
    #Restory every table if the corresponding file exists
    if not args.no_documents:
        print (blue("Compacting document table... ", bold=True))
        conn.compactRange(1)
    if not args.no_entities:
        print (blue("Compacting entity table... ", bold=True))
        conn.compactRange(2)
    if not args.no_document_idx:
        print (blue("Compacting document index table... ", bold=True))
        conn.compactRange(3)
    if not args.no_entity_idx:
        print (blue("Compacting entity index table... ", bold=True))
        conn.compactRange(4)


def truncate(args):
    "Delete data from one or more tables"
    #Check if the user is sure
    if not args.yes_i_know_what_i_am_doing:
        print (red("This will delete all your Translatron data. If you are sure, please use --yes-i-know-what-i-am-doing ", bold=True))
        return
    #Setup raw YakDB connection
    conn = YakDB.Connection()
    conn.connect(args.req_endpoint)
    #
    #Restory every table if the corresponding file exists
    if not args.no_documents:
        print (blue("Truncating document table... ", bold=True))
        if args.hard: conn.truncateTable(1)
        else: conn.deleteRange(1, None, None, None)
    if not args.no_entities:
        print (blue("Truncating entity table... ", bold=True))
        if args.hard: conn.truncateTable(2)
        else: conn.deleteRange(2, None, None, None)
    if not args.no_document_idx:
        print (blue("Truncating document index table... ", bold=True))
        if args.hard: conn.truncateTable(3)
        else: conn.deleteRange(3, None, None, None)
    if not args.no_entity_idx:
        print (blue("Truncating entity index table... ", bold=True))
        if args.hard: conn.truncateTable(4)
        else: conn.deleteRange(4, None, None, None)

def initializeTranslatron(args):
    import nltk
    nltk.download("all")

def runTranslatronCLI():
    """
    Call this function to use the yak CLI on sys.argv.
    """
    if sys.version_info.major < 3:
        print(red("Translatron requires Python 3 to run.\nPlease run translatron using a python3k interpreter!", bold=True))

    parser = argparse.ArgumentParser(description="Translatron client tool")
    # Database options
    serverArgsGroup = parser.add_argument_group(parser, "Translatron connection options")
    serverArgsGroup.add_argument(
            "-r", "--request-reply-endpoint",
            help="The endpoint for request-reply connections",
            action="store",
            default="ipc:///tmp/yakserver-rep",
            dest="req_endpoint")
    serverArgsGroup.add_argument(
            "-p", "--push-pull-endpoint",
            help="The endpoint for push-pull connections",
            action="store",
            default="ipc:///tmp/yakserver-pull",
            dest="push_endpoint")
    # CLI options
    cliOptsGroup = parser.add_argument_group(parser, "CLI options")
    # Data is remapped in connection class
    cliOptsGroup.add_argument(
            "-q", "--quiet",
            help="Don't print verbose info",
            action="store_true",
            dest="quiet")
    ###
    # Create parsers for the individual commands
    ###
    subparsers = parser.add_subparsers(title="Commands")
    # Connection check
    parserConnCheck = subparsers.add_parser("conncheck", description="Verify that a connection to YakDB is possible")
    parserConnCheck.set_defaults(func=checkConnection)
    # Run server
    parserRun = subparsers.add_parser("run", description="Run the Translatron server")
    parserRun.add_argument("--http-port", type=int, default=8080, help="Which port to listen on for HTTP requests")
    parserRun.set_defaults(func=runServer)
    # Indexer
    parserIndex = subparsers.add_parser("index", description="Run the indexer for previously imported documents")
    parserIndex.add_argument("--no-documents", action="store_true", help="Do not index documents")
    parserIndex.add_argument("--no-entities", action="store_true", help="Do not index entities")
    parserIndex.add_argument("-s", "--statistics", action="store_true", help="Print token frequency statistics")
    parserIndex.set_defaults(func=index)
    # Dump tables
    parserDump = subparsers.add_parser("dump", description="Export database dump")
    parserDump.add_argument("outprefix", default="translatron-dump", nargs='?', help="The file prefix to dump to. Table name and .xz is automatically appended")
    parserDump.add_argument("--no-documents", action="store_true", help="Do not dump the documents table")
    parserDump.add_argument("--no-entities", action="store_true", help="Do not dump the entity table")
    parserDump.add_argument("--no-document-idx", action="store_true", help="Do not dump the document index table")
    parserDump.add_argument("--no-entity-idx", action="store_true", help="Do not dump the entity index table")
    parserDump.add_argument("-x", "--xz", action="store_true", help="Use XZ compression instead of the default GZ")
    parserDump.set_defaults(func=exportDump)
    # Restore tables
    parserRestore = subparsers.add_parser("restore", description="Restore database dump (incremental)")
    parserRestore.add_argument("inprefix", default="translatron-dump", nargs='?', help="The file prefix to restore from. Table name and .xz is automatically appended")
    parserRestore.add_argument("--no-documents", action="store_true", help="Do not restore the documents table")
    parserRestore.add_argument("--no-entities", action="store_true", help="Do not restore the entity table")
    parserRestore.add_argument("--no-document-idx", action="store_true", help="Do not restore the document index table")
    parserRestore.add_argument("--no-entity-idx", action="store_true", help="Do not restore the entity index table")
    parserRestore.set_defaults(func=restoreDump)
    # Compact all tables
    parserCompact = subparsers.add_parser("compact", description="Perform a database compaction. Increases speed, but might take some time.")
    parserCompact.add_argument("--no-documents", action="store_true", help="Do not compact the documents table")
    parserCompact.add_argument("--no-entities", action="store_true", help="Do not compact the entity table")
    parserCompact.add_argument("--no-document-idx", action="store_true", help="Do not compact the document index table")
    parserCompact.add_argument("--no-entity-idx", action="store_true", help="Do not compact the entity index table")
    parserCompact.set_defaults(func=compact)
    # Truncate tables
    parserTruncate = subparsers.add_parser("truncate", description="Delete data from one or more tables")
    parserTruncate.add_argument("--no-documents", action="store_true", help="Do not truncate the documents table")
    parserTruncate.add_argument("--no-entities", action="store_true", help="Do not truncate the entity table")
    parserTruncate.add_argument("--no-document-idx", action="store_true", help="Do not truncate the document index table")
    parserTruncate.add_argument("--no-entity-idx", action="store_true", help="Do not truncate the entity index table")
    parserTruncate.add_argument("--yes-i-know-what-i-am-doing", action="store_true", help="Use this option if you are really sure you want to delete your data")
    parserTruncate.add_argument("--hard", action="store_true", help="Hard truncation (YakDB truncate instead of delete-range). Unsafe but faster and avoids required compaction. Server restart might be required")
    parserTruncate.set_defaults(func=truncate)
    # Import documents/entities from
    parserImportDocuments = subparsers.add_parser("import-documents", description="Import documents")
    parserImportDocuments.add_argument("infile", nargs="+", help="The PMC articles.X-Y.tar.gz input file(s)")
    parserImportDocuments.add_argument("-w", "--workers", type=int, default=cpu_count(), help="The number of worker processes to use")
    parserImportDocuments.add_argument("-f", "--filter", default="", help="Prefix filter for PMC TARs. For example, use ACS_Nano here to import only that journal")
    parserImportDocuments.add_argument("-c", "--content-filter", default="", help="Case-insensitive content filter for. For example, use Coxiella here to import only documents containing the string coxiella. Applied on the raw document.")
    parserImportDocuments.set_defaults(func=importDocuments)
    # Import documents/entities from
    parserImportEntities = subparsers.add_parser("import-entities", description="Import entities")
    parserImportEntities.add_argument("infile", nargs="+", help="The PMC articles.X-Y.tar.gz input file(s)")
    parserImportEntities.add_argument("-w", "--workers", type=int, default=cpu_count(), help="The number of worker processes to use")
    parserImportEntities.set_defaults(func=importEntities)
    # Intialize
    parserInitialize = subparsers.add_parser("initialize", description="Initialize translatron (download NLTK data)")
    parserInitialize.set_defaults(func=initializeTranslatron)
    # REPL
    parserREPL = subparsers.add_parser("repl", description="Start a Read-eval-print loop (REPL) for interactive DB usage")
    parserREPL.set_defaults(func=repl)
    ###
    # Parse and call the function
    ###
    args = parser.parse_args()
    # For some reason, the default=info setting only works with Python2
    if "func" not in args:
        print(red("No command specified, see help as listed below."))
        print(red("Example: translatron conncheck"))
        parser.print_help()
        sys.exit(1)
    args.func(args)
