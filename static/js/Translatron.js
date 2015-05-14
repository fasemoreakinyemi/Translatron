var Translatron = angular.module('Translatron', ['ui.bootstrap'])

function connectWebsocket() {
    console.info("Connecting to " + 'ws://' + window.location.hostname + ':9000')
    var conn = new WebSocket('ws://' + window.location.hostname + ':9000');
    conn.onerror = function (error) {
        console.log(error);
        alert("Error while communicating with server: " + JSON.stringify(error));
    };
    return conn;
}

/**
 * Defines how NER highlight results are rendered,
 * depending on the source database. By default, labels are rendered
 * gray (label-default). If you want a different rendering for a special DB,
 * add it here.
 */
var dbToLabelColor = {
    "MeSH": "label-info",
    "Wikipedia": "label-warning",
    "UniProt": "label-success",
}

Translatron.controller('SearchCtrl', ["$scope", "$http", "$modal", "$log", function ($scope, $http, $modal, $log) {
    $scope.searchResults = [];
    $scope.nerResults = {};

    /**
     * Rendering a large number of search results is quite slow.
     * Usually, rendering would take place for every single search character.
     * Therefore, the apparent performance decreases significantly due to the single-threaded
     * nature of JavaScript.
     * 
     * This counter is incremented for every search request made.
     * It is decremented for every search results received.
     * Only if the (pre-decremented) value is zero the search results are actually displayed.
     * This ensures that (even though the server has the same load) only the last result
     * is displayed.
     */
    $scope.searchRequestsInQueue = 0;

    /*
     * Setup websocket connection
     */
    $scope.connection = connectWebsocket();

    $scope.connection.onmessage = function (message) {
        var response = JSON.parse(message.data);
        if(response.qtype =="docsearch") {
            //See $scope.searchRequestsInQueue declaration for description on how we do this
            $scope.searchRequestsInQueue--;
            if($scope.searchRequestsInQueue > 0) {
                return; //Ignore search results... render next one.
            }

            $scope.searchResults = response.results;
            $scope.$apply();
        } else if (response.qtype == "ner") {
            console.log(response.results)
            jQuery.extend($scope.nerResults, response.results);
            //Find the correct document for the query
            var docElem = $(".results").find('[data-docid="' + response.docid + '"]')
            var paragraphs = $(docElem).find(".paragraph")
            //User jquery.highlight to find and (invisibly) mark the hit with .highlight
            for (var key in response.results) {
                paragraphs.highlight(key, {wordsOnly: true, caseSensitive: true})
                var dbid = response.results[key][0]; //E.g. "Poisson Distribution"
                var dbName = response.results[key][1]; //E.g. "MeSH"
                //Compute label color, i.e. highlight specific databases.
                //NOTE: The server always takes the FIRST hit. Therefore there might be cases
                // when the correct highlighting for an ID does not apply because a different
                // database was the first one.
                var labelColor = dbToLabelColor[dbName];
                if(labelColor === undefined) {
                    labelColor = "label-default"
                }
                /** 
                 * Process highlighted tags
                 */
                $(".highlight").each(function(index) {
                    //Do not add label if NER was performed multiple times
                    if($(this).parent().hasClass("label")) { //Only remove .highlight div
                        $(this).replaceWith(this.innerHTML)
                    } else { //We're not already inside a label. Add a label
                        //Link to the entity page, with the search term set to the token name
                        var href = "/entities.html#" + encodeURI(dbid);
                        var elem = $('<a href="' + href + '" target="_blank"><span class="label '
                                     + labelColor + ' ner-result">' + this.innerHTML + '</span></a>');
                        $(this).replaceWith(elem)
                    }
                });
            }
        } else if (response.qtype == "getdocuments") {
            //Usually only one document
            for (var i = response.results.length - 1; i >= 0; i--) {
                var result = response.results[i];
                // Replace all search results with the same ID
                for (var j = $scope.searchResults.length - 1; j >= 0; j--) {
                    if($scope.searchResults[j].id == result.id) {
                        $scope.searchResults[j] = result
                    }
                };
            };
            //Re-render
            $scope.$apply();
        }
    }

    $scope.performSearch = function () {
        searchObj = {
            "qtype": "docsearch",
            "term": $scope.searchExpression
        }
        $scope.searchRequestsInQueue++;
        $scope.connection.send(JSON.stringify(searchObj));
        //Result is handled in onmessage / onerror
    };

    $scope.performNER = function (doc) {
        searchObj = {
            "qtype": "ner",
            "query": doc.paragraphs.join("\n"),
            "docid": doc.id
        }
        $scope.connection.send(JSON.stringify(searchObj));
    }

    $scope.showFullDocument = function(doc) {
        searchObj = {
            "qtype": "getdocuments",
            "query": [doc.id]
        }
        $scope.connection.send(JSON.stringify(searchObj));
        
    }
}]);

Translatron.filter('authors', function() {
    return function(input) {
        if(input === undefined || input.length == 0) {
            return "";4
        }
        input = input.slice(0, 5);
        str = ""
        for (var i = 0; i < input.length; i++) {
            if(i == input.length - 1 && input.length > 1) {
                str += " & ";
            }
            else if(i != 0) {str += ", ";}
            str += input[i];
        };

        return str;
    };
});


/**
 * Generates a database reference URL from a UniProt meta-database URL template
 */
Translatron.filter('dbref', function() {
    return function(id, urltemplate) {
        var encoded = encodeURI(id);
        return urltemplate.replace("%s", encoded).replace("%u", encoded)
    };
});


/**
 * Utility to define filters for simple prefix-based links like DOI, PubMed, PMC etc
 */
function defineSimpleLink(name, prefix) {
    Translatron.filter(name, function() {
        return function(id) {
            if(id === undefined) {
                return undefined;
            }
            return prefix + id;
        };
    });
}

defineSimpleLink("DOILink", "https://dx.doi.org/")
defineSimpleLink("PubMedLink", "https://www.ncbi.nlm.nih.gov/pubmed/")
defineSimpleLink("PMCLink", "https://www.ncbi.nlm.nih.gov/pmc/articles/")

Translatron.directive('linklabel', function () {
    return {
        restrict: 'E',
        scope: {
            color: '@',
            href: '@',
            label: '@'
        },
        template: '<a ng-if="href !== undefined" href="{{href}}" target="_blank" ng-show="{{href.length > 0}}"><span class="label label-{{color}}">{{label}}</span></a>',
    }
});

Translatron.controller('EntityCtrl', ["$scope", "$http", "$modal", "$log", "$location", function ($scope, $http, $modal, $log, $location) {
    $scope.searchResults = [];
    $scope.connection = connectWebsocket();
    //Immediately request meta database & process search expression from URL
    $scope.connection.onopen = function() {
        $scope.connection.send(JSON.stringify({"qtype":"metadb"}));
        // Raw hash part of URL: /entities.html#foobar -> foobar --> search expression
        var urlTerm = $location.path().substring(1)
        if(urlTerm) {
            //Perform search with URL-defined term
            $scope.searchExpression = urlTerm;
            $scope.performSearch();
            //Apply
            $scope.$apply();
        }
    }

    /**
     * Meta-database (mainly from UniProt - see UniprotMetadatabase.py)
     * This will be requested from the server via WebSockets.
     * In case of request failure, this will be empty.
     */
    $scope.metaDB = {};
    
    $scope.performSearch = function() {
        searchObj = {
            "qtype": "entitysearch",
            "term": $scope.searchExpression
        }
        $scope.connection.send(JSON.stringify(searchObj));
    }

    $scope.connection.onmessage = function (message) {
        var response = JSON.parse(message.data);
        if(response.qtype == "entitysearch") {
            $scope.searchResults = response.entities;
            $log.info($scope.searchResults)
            $scope.$apply();
        } else if(response.qtype == "metadb") {
            $scope.metaDB = response.results;
        }
    }

    $scope.clickSearch = function(refId, $event) {
        if($event.shiftKey) {
            $event.preventDefault();
            $scope.searchExpression = refId;
            $scope.performSearch();
        }
    }
}]);
