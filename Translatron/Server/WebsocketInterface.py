#!/usr/bin/env python3
from autobahn.asyncio.websocket import WebSocketServerProtocol, \
    WebSocketServerFactory
from Translatron.DocumentDB import YakDBDocumentDatabase, documentSerializer
from nltk.tokenize.regexp import RegexpTokenizer
from nltk.tokenize import word_tokenize
try:
    import simplejson as json
except ImportError:
    import json
import threading
import time
from ansicolor import blue, yellow, red
from YakDB.InvertedIndex import InvertedIndex
from Translatron.Misc.UniprotMetadatabase import initializeMetaDatabase


def has_alpha_chars(string):
    return any((ch.isalnum() for ch in string))


# Initialize objects that will be passed onto the client upon request
metaDB = initializeMetaDatabase()


class TranslatronProtocol(WebSocketServerProtocol):
    def __init__(self):
        """Setup a new connection"""
        print(yellow("Initializing new YakDB connection"))
        self.db = YakDBDocumentDatabase()
        # Initialize NLTK objects
        self.nerTokenizer = RegexpTokenizer(r'\s+', gaps=True)

    def onConnect(self, request):
        pass

    def onOpen(self):
        pass

    def performDocumentSearch(self, query):
        """
        Perform a token search on the document database.
        Search is performed in multi-token prefix (all must hit) mode.
        Tokens with no hits at all are ignored entirely
        """
        startTime = time.time()
        queryTokens = map(str.lower, word_tokenize(query))
        levels = [b"title", b"content", b"metadata"]
        #Remove 1-token parts from the query -- they are way too general!
        #Also remove exclusively-non-alnum tokens
        queryTokens = [tk for tk in queryTokens if (len(tk) > 1 and has_alpha_chars(tk))]
        results = self.db.searchDocumentsMultiTokenPrefix(queryTokens, levels=levels)
        #Return only those paragraphs around the hit paragraph (or the first 3 pararaphs)
        for hitLocation, doc in results.items():
            (docId, docLoc) = InvertedIndex.splitEntityIdPart(hitLocation)
            #Compute which paragraphs to display
            minShowPar = 0
            maxShowPar = 2
            if docLoc.startswith(b"paragraph"):
                paragraphNo = int(docLoc[9:])
                minShowPar = max(0, paragraphNo - 1)
                maxShowPar = min(len(doc[b"paragraphs"]), paragraphNo + 1)
            #Modify documents
            results[hitLocation][b"hitLocation"] = docLoc
            results[hitLocation][b"paragraphs"] = doc[b"paragraphs"][minShowPar:maxShowPar]
        # Measure timing
        timeDiff = (time.time() - startTime) * 1000.0
        print("Document search for %d tokens took %.1f milliseconds" % (len(queryTokens), timeDiff))
        return results

    def uniquifyEntities(self, entities):
        """Remove duplicates from a list of entities (key: ["id"])"""
        seen = set()
        result = []
        for entity in entities:
            itemId = entity[b"id"]
            if itemId in seen: continue
            seen.add(itemId)
            result.append(entity)
        return result

    def performEntitySearch(self, query):
        """
        Search entities. Tokens are not splitted in order to allow simple search
        for multi-token entities like "Biological process"
        """
        results = self.db.searchEntitiesSingleTokenMultiExact([query], level=b"aliases")
        #Return only result array. TODO can't we just use results[query]
        if query not in results:
            return []
        return results[query]

    def filterNERTokens(self, token):
        """
        Filter function to remove stuff that just clutters the display.
        """
        #Short numbers are NOT considered database IDs.
        #NOTE: In reality, pretty much all numbers are Allergome database IDs, e.g. see
        # http://www.allergome.org/script/dettaglio.php?id_molecule=14
        if len(token) <= 5 and token.isdigit():
            return False
        return True

    def performEntityNER(self, query):
        "Search a query text for entity/entity alias hits"
        startTime = time.time()
        tokens = self.nerTokenizer.tokenize(query)
        queryTokens = [s.encode("utf-8") for s in tokens]
        # Search for case-sensitive hits
        searchFN = InvertedIndex.searchSingleTokenMultiExact
        results = searchFN(self.db.entityIdx.index, frozenset(filter(self.filterNERTokens, queryTokens)), level=b"aliases")
        # Results contains a list of tuples (dbid, db) for each hit. dbid is db + b":" + actual ID
        # We only need the actual ID, so remove the DBID prefix (which is required to avoid inadvertedly merging entries).
        # This implies that the DBID MUST contain a colon!
        results =  {k: [(a.partition(b":")[2], b) for (a, b) in v] for k, v in results.items() if v}
        #
        # Multi-token NER
        # Based on case-insensitive entries where only the first token is indexed.
        #
        # TESTING: Multi token NER
        lowercaseQueryTokens = [t.lower() for t in queryTokens]
        t1 = time.time()
        ciResults = searchFN(self.db.entityIdx.index, frozenset(lowercaseQueryTokens), level=b"cialiases")
        t2 = time.time()
        print("TX " + str(t2 - t1))
        for (firstTokenHit, hits) in ciResults.items():
            #Find all possible locations where the full hit could start, i.e. where the first token produced a hit
            possibleHitStartIndices = [i for i, x in enumerate(lowercaseQueryTokens) if x == firstTokenHit]
            #Iterate over all possible
            for hit in hits:
                hitLoc, _, hitStr = hit[1].rpartition(b"\x1D") # Full (whitespace separated) entity name
                if not hitStr: continue #Ignore malformed entries. Should usually not happen
                hitTokens = [t.lower() for t in hitStr.split()]
                numTokens = len(hitTokens)
                #Check if at any possible hit start index the same tokens occur (in the same order )
                for startIdx in possibleHitStartIndices:
                    actualTokens = lowercaseQueryTokens[startIdx : startIdx+numTokens]
                    #Check if the lists are equal. Shortcut for single-token hits
                    if numTokens == 1 or all((a == b for a, b in zip(actualTokens, hitTokens))):
                        #Reconstruct original (case-sensitive) version of the hit
                        csTokens = queryTokens[startIdx : startIdx+numTokens]
                        #NOTE: This MIGHT cause nothing to be highlighted, if the reconstruction
                        # of the original text is not equal to the actual text. This is true exactly
                        # if the tokenizer removes or changes characters besides whitespace in the text.
                        csHit = b" ".join(csTokens)
                        # Emulate defaultdict behaviour
                        if not csHit in results: results[csHit] = []
                        results[csHit].append((hitStr, hitLoc))
        t3 = time.time()
        print("TY " + str(t3 - t2))
        # TODO: Remove results which are subsets of other hits. This occurs only if we have multi-token results
        removeKeys = set() # Can't modify dict while iterating it, so aggregate keys to delete
        for key in results.keys():
            # Ignore single part results
            if any((chr(c).isspace() for c in key)):
                tokens = key.split()
                for token in tokens:
                    # Remove sub-hit in results.
                    # This avoids the possibility of highlighting the smaller hit
                    if token in results:
                        removeKeys.add(token)
        # Remove aggregated keys
        for key in removeKeys:
            del results[key]
        # Result: For each token with hits --> (DBID, Database name)
        # Just takes the first DBID.It is unlikely that different DBIDs are found, but we
        #   can only link to one using the highlighted label
        ret =  {k: (v[0][0], v[0][1]) for k, v in results.items() if v}
        # Measure timing
        timeDiff = (time.time() - startTime) * 1000.0
        print("NER for %d tokens took %.1f milliseconds" % (len(queryTokens), timeDiff))
        return ret


    def onMessage(self, payload, isBinary):
        request = json.loads(payload.decode('utf8'))
        # Perform action depending on query type
        qtype = request["qtype"]
        if qtype == "docsearch":
            results = self.performDocumentSearch(request["term"])
            del request["term"]
            request["results"] = list(results.values())
        elif qtype == "ner":
            results = self.performEntityNER(request["query"])
            del request["query"]
            request["results"] = results
        elif qtype == "metadb":
            # Send meta-database to generate
            request["results"] = metaDB
        elif qtype == "entitysearch":
            request["entities"] = self.performEntitySearch(request["term"])
            del request["term"]
        elif qtype == "getdocuments":
            # Serve one or multiple documents by IDs
            docIds = [s.encode() for s in request["query"]]
            request["results"] = self.db.docIdx.findEntities(docIds)
            del request["query"]
        else:
            print(red("Unknown websocket request type: %s" % request["qtype"], bold=True))
            return # Do not send reply
        #Return modified request object: Keeps custom K/V pairs but do not re-send query
        self.sendMessage(json.dumps(request, default=documentSerializer).encode("utf-8"), False)

    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {0}".format(reason))


def startWebsocketServer():
    print(blue("Websocket server starting up..."))

    try:
        import asyncio
    except ImportError:
        ## Trollius >= 0.3 was renamed
        import trollius as asyncio

    #Asyncio only setups an event loop in the main thread, else we need to
    if threading.current_thread().name != 'MainThread':
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    factory = WebSocketServerFactory("ws://0.0.0.0:9000", debug = False)
    factory.protocol = TranslatronProtocol

    loop = asyncio.get_event_loop()
    server = loop.create_server(factory, '0.0.0.0', 9000)
    server = loop.run_until_complete(server)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        loop.close()
