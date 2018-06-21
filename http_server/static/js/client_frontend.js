(function(){

    RDFaParser = {};

    RDFaParser.parseResource = function(resource,blankPrefix, graph, defaultSubject) {
        var currentUri = jQuery.uri.base().toString();
        if(currentUri.indexOf("#") != -1) {
            currentUri = currentUri.split("#")[0];
        }

        if(resource.type === 'uri') {
            if(resource.value._string.indexOf(currentUri) != -1) {
                var suffix = resource.value._string.split(currentUri)[1];
                var defaultPrefix = defaultSubject.toString();
                if(suffix != "") {
                    defaultPrefix = defaultPrefix.split("#")[0]
                }
                return {'uri': defaultPrefix+suffix};
            } else {
                var uri = resource.value._string;
                if(uri.indexOf('file:') === 0){
                    uri = defaultSubject.scheme + '://' + defaultSubject.authority + uri.replace('file:','');
                }
                return {'uri': uri};
            }
        } else if(resource.type === 'bnode') {
            var tmp = resource.toString();
            if(tmp.indexOf("_:")===0) {
                return {'blank': resource.value + blankPrefix };
            } else {
                return {'blank': "_:"+tmp};
            }

        } else if(resource.type === 'literal') {
            return {'literal': resource.toString()};
        }
    };

    RDFaParser.parseQuad = function(graph, parsed, blankPrefix, defaultSubject) {
        var quad = {};
        quad['subject'] = RDFaParser.parseResource(parsed.subject, blankPrefix, graph, defaultSubject);
        quad['predicate'] = RDFaParser.parseResource(parsed.property, blankPrefix, graph, defaultSubject);
        quad['object'] = RDFaParser.parseResource(parsed.object, blankPrefix, graph, defaultSubject);
        quad['graph'] = graph;

        return quad;
    };

    RDFaParser.parse = function(data, graph, options, callback) {
        var nsRegExp = /\s*xmlns:(\w*)="([^"]+)"\s*/i;
        var ns = {};

        // some default attributes
        ns['og'] = jQuery.uri("http://ogp.me/ns#");
        ns['fb'] = jQuery.uri("http://www.facebook.com/2008/fbml");

        var baseRegExp  = /\s*xmlns="([^"]+)"\s*/i
        var baseMatch = baseRegExp.exec(data);

        if(baseMatch != null) {
            window['rdfaDefaultNS'] = jQuery.uri(baseMatch[1]);
        }

        var tmp = data;
        var match = nsRegExp.exec(tmp);
        var index = null;
        while(match != null) {
            ns[match[1]] = jQuery.uri(match[2]);
            tmp = tmp.slice(match.index+match[0].length, tmp.length);
            match = nsRegExp.exec(tmp);
        }

        window['globalNs'] = ns;

        var rdfa = jQuery(data).rdfa();
        var parsed = rdfa.databank.triples();
        var quads = [];
        var prefix = ""+(new Date()).getTime();
        for(var i=0; i<parsed.length; i++) {
            quads.push(RDFaParser.parseQuad(graph,parsed[i],prefix, window['rdfaCurrentSubject']));
        }

        callback(null, quads);
    };

    // RDFParser
    RDFParser = {};

    RDFParser.parse = function(data, graph) {
        var parsed = jQuery().rdf().databank.load(data).triples();
        var quads = [];
        var prefix = ""+(new Date()).getTime();
        for(var i=0; i<parsed.length; i++) {
            quads.push(RDFaParser.parseQuad(graph,parsed[i],prefix, window['rdfaCurrentSubject']));
        }

        return quads;
    };

    jQuery.fn.center = function () {
        this.css("position","absolute");

        var bounds = jQuery("#client-frontend").position();
        var height = jQuery("#client-frontend").height();
        var width = jQuery("#client-frontend").width();
        this.css("top", ((height - bounds.top) / 2) + $(window).scrollTop() - (this.height()/2) + "px");
        this.css("left", ((width - bounds.left) / 2) + $(window).scrollLeft() - (this.width()/2) + "px");

        return this;
    };

    ClientFrontend = function(node,store) {
        var html = "<div id='client-frontend' class='container'><h1>Sage JS Client</h1><br/>";
        html = html + "<div id='client-frontend-overlay'></div>"
        html = html + "<div id='client-frontend-server-title'><strong>Server :</strong></div><div id='client-frontend-server-area'></div>";
        html = html + "<div id='client-frontend-query-title'><strong>Query :</strong></div><div id='client-frontend-query-area'></div>";
        html = html + "<div id='client-frontend-menu'></div><br/>";
        html = html + "<div id='client-frontend-results-area' class='table-responsive'></div>";
        html = html + "<div id='client-frontend-footer'>";
        html = html + "<table class='table table-borderless'><tr>";
        html = html + "<td id='btn-prev'><button id='client-frontend-prev-image-placeholder' class='btn btn-primary' data-bind='click:prevResultPage'>\<</button></td>";
        html = html + "<td id='lbl-pages'><label class='rdfstore-footer-info' id='client-frontend-footer-display-pages' data-bind='text:\"Page \"+currentResultPage()+\"/\"+totalResultPages()+\", (\"+allBindings().length+\" results)\"'></label></td>";
        html = html + "<td 'btn-next'><button id='client-frontend-next-image-placeholder' class='btn btn-primary' data-bind=' click:nextResultPage'>\></button></td>";
        html = html + "</tr></table>";
        html = html + "</div>";
        html = html + "</div>"; // client-frontend

        jQuery(node).append(html);
        jQuery('#client-frontend-next-image-placeholder').hide();
        jQuery('#client-frontend-prev-image-placeholder').hide();

        this.buildTemplates(node);
        this.buildServerArea();
        this.buildQueryArea();
        this.buildMenu();
        this.buildResultsArea();

        // application handler;
        this.viewModel.application = this;

        // save the root node
        this.viewModel.rootNode = node;

        // save the store
        this.viewModel.store = store;

        this.viewModel.bindingsVariables = ko.dependentObservable(function(){
                                                                      var array = new Array();
                                                                      if(this.bindings().length === 0 || this.bindings() == null) {
                                                                          return [];
                                                                      } else {
                                                                          var sample = this.bindings()[0];
                                                                          for(var p in sample) {
                                                                              array.push(p);
                                                                          }
                                                                          return array;
                                                                      }
                                                              },this.viewModel);

        this.viewModel.bindingsArray = ko.dependentObservable(function(){
                                                                  var array = new Array();
                                                                  for(var i=0; i<this.bindings().length; i++) {
                                                                      var currentBindings = this.bindings()[i];
                                                                      var nextElement = new Array();
                                                                      for(var j=0; j<this.bindingsVariables().length; j++) {
                                                                          nextElement.push(currentBindings[this.bindingsVariables()[j]]);
                                                                      };
                                                                      array.push(nextElement);
                                                                  }
                                                                  return array;
                                                              },this.viewModel);

        ko.applyBindings(this.viewModel, jQuery(node).get(0));
    };

    ClientFrontend.prototype.buildTemplates = function(node) {

        html = "<script id='sparql-results-template' type='text/html'><table id='sparql-results-table-headers' class='table table-striped table-hover'><thead><tr>{{each bindingsVariables}}";
        html = html + "<th scope='col'>${$value}</th>{{/each}}</tr></thead><tbody>{{each bindingsArray}}";
        html = html + "<tr class='{{if $index%2==0}}sparql-result-even-row{{else}}sparql-result-odd-row{{/if}}'>{{each $value }}{{if $value.token==='uri'}}";
        html = html + "<td data-bind='click: newShowBinding, event: {mouseover: tdMouseOver}'><span class='rdfstore-data-value'>${$value.value}</span>";
        html = html + "<span class='rdfstore-data-token' style='display:none'>uri</span></td>{{else}}{{if $value.token==='literal'}}";
        html = html + "<td data-bind='click: newShowBinding, event: {mouseover: tdMouseOver}'><span class='rdfstore-data-value'>${$value.value}</span>";
        html = html + "<span class='rdfstore-data-token' style='display:none'>literal</span><span class='rdfstore-data-lang'  style='display:none'>${$value.lang}</span>";
        html = html + "<span class='rdfstore-data-type'  style='display:none'>${$value.type}</span></td>{{else}}<td data-bind='click: newShowBinding, event: {mouseover: tdMouseOver}'>";
        html = html + "<span class='rdfstore-data-value'>${$value.value}</span><span class='rdfstore-data-token' style='display:none'>blank</span></td>";
        html = html + "{{/if}}{{/if}}{{/each}}</tr>{{/each}}</tbody></table></script>";

        jQuery(node).append(html);

        html = "<script id='sparql-template-row' type='text/html'><td>${$item.data}</td></script>";

        jQuery(node).append(html);
    };

    ClientFrontend.prototype.buildMenu = function() {
        var html = "<div id='rdf-store-menu'>";
        html = html + "<div id='rdf-store-menu-run' class='rdf-store-menu-action'><button type='button' class='btn btn-primary' href='#' data-bind='click:submitQuery'>Execute</button>&nbsp;&nbsp;&nbsp;<button type='button' id='loadingBtn' class='btn btn-warning' href='#' disabled><i class='fas fa-sync fa-spin'></i>&nbsp;&nbsp;Loading</button></div>";
        jQuery('#client-frontend-menu').append(html);
        jQuery('#loadingBtn').hide();
    };

    ClientFrontend.prototype.buildServerArea = function() {
        var html = '<div class="input-group mb-3">\
        <div class="input-group-prepend">\
          <button class="btn btn-outline-secondary dropdown-toggle" type="button" id="dropdownMenuButton" data-toggle="dropdown" aria-haspopup="true" aria-expanded="false">Sage Servers</button>\
          <div class="dropdown-menu" id="datasetList">\
          </div>\
        </div>\
            <input type="text" class="form-control" id="sparql-server-text" data-bind="value:server"/>\
        </div>'
		    jQuery('#client-frontend-server-area').append(html);
		};

    ClientFrontend.prototype.buildQueryArea = function() {
        var html = "<textarea id='sparql-query-text' class='form-control' data-bind='text:query'></textarea>";
        jQuery('#client-frontend-query-area').append(html);
    };

    ClientFrontend.prototype.buildResultsArea = function() {
        var html = "<div id='client-frontend-query-results' data-bind='template:{name:\"sparql-results-template\"}'></div>";
        jQuery('#client-frontend-results-area').append(html);
    };

    ClientFrontend.prototype.showUriDialogModel = {
        create: function(viewModel, value) {
            this.value = value;
            this.viewModel = viewModel;
            this.id = "rdf-store-menu-show-uri-dialog"+(new Date().getTime());
            var html = "<div id='"+this.id+"' class='rdf-store-dialog'>";
            html = html + "<div class='rdfstore-dialog-title'><p>"+value+"</p></div>";
            html = html + "<div class='rdfstore-dialog-row'><span>URI:</span><input id='rdf-store-show-uri-value' type='text' value='"+value+"'></input></div>";
            html = html + "<div id='rdfstore-show-uri-row-options' class='rdfstore-options-row'>";

            html = html + "<div class='rdfstore-options-row-item' id='rdf-store-dialog-browse-uri' data-bind='click: browseUri'>browse</div>";
            html = html + "<div class='rdfstore-options-row-item' id='rdf-store-dialog-browse-store' data-bind='click: storeUri'>load</div>";
            html = html + "</div>";
            html = html + "<div class='rdfstore-dialog-actions'>";
            html = html + "<input type='submit' value='cancel' style='float:none; min-width:100px' data-bind='click:closeDialog'></input>";
            html = html + "</div>";
            html = html + "</div>";

            jQuery(viewModel.rootNode).append(html);
            jQuery("#"+this.id).css("min-height", "280px").css("height", "280px").center();

            ko.applyBindings(this, jQuery("#"+this.id).get(0));
            jQuery("#"+this.id).draggable({handle: "div.rdfstore-dialog-title"});
        },

        closeDialog: function() {
            // modal
            jQuery('#client-frontend-overlay').hide();

            jQuery("#"+this.id).remove();
        },

        browseUri: function() {
            window.open(this.value, "Browse: "+this.value);
        },

        storeUri: function() {
            this.closeDialog();
            this.application.loadGraphDialogModel.create(this.viewModel, this.value);
            this.application.loadGraphDialogModel.application = this.application;
            this.application.loadGraphDialogModel.store = this.store;
        }

    };

    ClientFrontend.prototype.showLiteralDialogModel = {
        create: function(viewModel, value, lang, type) {
            this.value = value;
            this.viewModel = viewModel;
            this.id = "rdf-store-menu-show-literal-dialog"+(new Date().getTime());
            var html = "<div id='"+this.id+"' class='rdf-store-dialog'>";
            html = html + "<div class='rdfstore-dialog-title'><p>Show Literal</p></div>";
            html = html + "<div class='rdfstore-dialog-row'><span>Type:</span><input id='rdf-store-show-literal-type' type='text' value='"+type+"'></input></div>";
            html = html + "<div class='rdfstore-dialog-row'><span>Language:</span><input id='rdf-store-show-literal-language' type='text' value='"+lang+"'></input></div>";
            html = html + "<div class='rdfstore-dialog-row'><span>Value:</span><textarea id='rdf-store-show-literal-value' type='text'>"+value+"</textarea></div>";
            html = html + "<div class='rdfstore-dialog-actions' id='rdfstore-dialog-actions-show-literal'>";
            html = html + "<input type='submit' value='cancel' style='float:none; min-width:100px' data-bind='click:closeDialog'></input>";
            html = html + "</div>";
            html = html + "</div>";

            jQuery(viewModel.rootNode).append(html);
            jQuery("#"+this.id).css("min-height", "380px").css("height", "380px").center();

            ko.applyBindings(this, jQuery("#"+this.id).get(0));
            jQuery("#"+this.id).draggable({handle: "div.rdfstore-dialog-title"});
        },

        closeDialog: function() {
            // modal
            jQuery('#client-frontend-overlay').hide();

            jQuery("#"+this.id).remove();
        },

        browseUri: function() {
            window.open(this.value, "Browse: "+this.value);
        },

        storeUri: function() {
            this.closeDialog();
            this.application.loadGraphDialogModel.create(this.viewModel, this.value);
            this.application.loadGraphDialogModel.application = this.application;
            this.application.loadGraphDialogModel.store = this.store;
        }

    };

    ClientFrontend.prototype.loadGraphDialogModel = {
        create: function(viewModel, uriToLoad) {
            // modal
            jQuery('#client-frontend-overlay').show();

            this.viewModel = viewModel;

            var html = "<div id='rdf-store-menu-load-dialog' class='rdf-store-dialog'>";
            html = html + "<div class='rdfstore-dialog-title'>Load remote graph</div>";
            if(uriToLoad) {
                html = html + "<div class='rdfstore-dialog-row'><span>Graph to load URI:</span><input id='rdf-store-graph-to-load' type='text' value='"+uriToLoad+"'></input></div>";
            } else {
                html = html + "<div class='rdfstore-dialog-row'><span>Graph to load URI:</span><input id='rdf-store-graph-to-load' type='text'></input></div>";
            }
            html = html + "<div class='rdfstore-dialog-row'><span>Store graph URI:</span><input id='rdf-store-graph-to-store' type='text'></input></div>";
            html = html + "<div class='rdfstore-dialog-actions'>";
            html = html + "<input type='submit' value='cancel' style='float:none; min-width:100px' data-bind='click:closeDialog'></input>";
            html = html + "</div>";
            html = html + "</div>";

            jQuery(viewModel.rootNode).append(html);
            jQuery("#rdf-store-menu-load-dialog").css("min-height", "180px").css("height", "180px").center();

            ko.applyBindings(this, jQuery("#rdf-store-menu-load-dialog").get(0));
            jQuery("#rdf-store-menu-load-dialog").draggable({handle: "div.rdfstore-dialog-title"});
        },

        closeDialog: function() {
            // modal
            jQuery('#client-frontend-overlay').hide();
            jQuery("#rdf-store-menu-load-dialog").remove();
            jQuery("#rdfstore-dialog-load-submit-btn").attr('disabled',false);
        },



    };

    ClientFrontend.prototype.viewModel = {
        rootNode: null,

         modified: true,
         lastQuery: null,


         server: ko.observable('https://sage.univ-nantes.fr/sparql/watdiv10m'),

         query: ko.observable('SELECT * WHERE {\n\t?v0 <http://purl.org/goodrelations/includes> ?v1 .\n\t?v1 <http://schema.org/contentSize> ?v3 .\n\t?v0 <http://schema.org/eligibleRegion> <http://db.uwaterloo.ca/~galuc/wsdbm/Country9>.\n}'),

         prevHistory: ko.observable([]),

         nextHistory: ko.observable([]),

         bindingsPerPage: ko.observable(50),

         allBindings: ko.observable([]),

         bindings: ko.observable([]),

         totalResultPages: ko.observable(0),

         currentResultPage: ko.observable(0),

         prevResultPage: function() {
             var currentResultPage = this.currentResultPage();
             var maxPages = Math.ceil(this.allBindings().length / this.bindingsPerPage());
             if(currentResultPage > 1) {
                 currentResultPage = currentResultPage - 1;
                 var startBindings = (currentResultPage-1) * this.bindingsPerPage();
                 this.currentResultPage(currentResultPage);
                 this.bindings(this.allBindings().slice(startBindings, startBindings+this.bindingsPerPage()));
                 if (currentResultPage === 1) {
                   this.togglePrevPage();
                 }
                 if (currentResultPage === (maxPages - 1)) {
                   this.toggleNextPage();
                 }
             }
         },

        toggleNextPage: function() {
            jQuery('#client-frontend-next-image-placeholder').toggle();
        },
        maybeToggleNextPage: function() {
            if(jQuery("#client-frontend-next-image-placeholder").attr('class') === 'rdfstore-next-image-mousedown') {
                jQuery("#client-frontend-next-image-placeholder").attr('class','');
            }
        },

        togglePrevPage: function() {
            jQuery('#client-frontend-prev-image-placeholder').toggle();
        },

        maybeTogglePrevPage: function() {
            if(jQuery("#client-frontend-prev-image-placeholder").attr('class') === 'rdfstore-prev-image-mousedown') {
                jQuery("#client-frontend-prev-image-placeholder").attr('class','');
            }
        },

        nextResultPage: function() {
            var currentResultPage = this.currentResultPage();
            var maxPages = Math.ceil(this.allBindings().length / this.bindingsPerPage());
            if(currentResultPage<maxPages) {
                var startBindings = currentResultPage * this.bindingsPerPage();
                currentResultPage = currentResultPage + 1;
                this.currentResultPage(currentResultPage);
                this.bindings(this.allBindings().slice(startBindings, startBindings+this.bindingsPerPage()));
                if (currentResultPage === maxPages) {
                  this.toggleNextPage();
                }
                if (currentResultPage === 2) {
                  this.togglePrevPage();
                }
            }
        },

        submitQuery: function() {
            var query = jQuery('#sparql-query-text').val();
            var server = jQuery('#sparql-server-text').val();
            jQuery('#client-frontend-next-image-placeholder').hide();
            jQuery('#client-frontend-prev-image-placeholder').hide();
            var that = this;
            var callback = function(err,results){
                if(!err) {
                    if(that.lastQuery == null) {
                        that.lastQuery = query;
                        that.modified = false;
                    }

                    that.allBindings(results || []);
                    that.bindings(results.slice(0,that.bindingsPerPage()));
                    that.totalResultPages(Math.ceil(results.length/that.bindingsPerPage()))
                    that.currentResultPage(1);
                    var maxPages = Math.ceil(that.allBindings().length / that.bindingsPerPage());
                    if (maxPages>1) {
                      that.toggleNextPage();
                    }
                    jQuery('#loadingBtn').hide();
                } else {
                    alert("Error executing query: "+results);
                }
            };
            this.store.execute(query, callback, server);
            jQuery('#loadingBtn').show();
        },

        tdMouseOver: function(event) {
            jQuery('td.rdfstore-td-over').attr('class','');
            jQuery(event.currentTarget).attr('class', 'rdfstore-td-over');
        },

        mouseOverMinWindow: function(event) {
            var current = jQuery('#rdf-store-min-window').attr('class');
            if(current.indexOf("rdfstore-window-button-over") == -1) {
                jQuery('#rdf-store-min-window').attr('class', "rdfstore-window-button rdfstore-window-button-over");
            } else {
                jQuery('#rdf-store-min-window').attr('class', "rdfstore-window-button");
            }
        },

        mouseOverCloseWindow: function(event) {
            var current = jQuery('#rdfstore-close-window').attr('class');
            if(current.indexOf("rdfstore-window-button-over") == -1) {
                jQuery('#rdfstore-close-window').attr('class', "rdfstore-window-button rdfstore-window-button-over");
            } else {
                jQuery('#rdfstore-close-window').attr('class', "rdfstore-window-button");
            }
        },

        closeWindow: function() {
            jQuery("#client-frontend").remove();
        },

        newShowBinding: function(event) {
            // modal
            jQuery('#client-frontend-overlay').show();

            var kind = jQuery(event.currentTarget).find("span.rdfstore-data-token").text()
            var value = jQuery(event.currentTarget).find("span.rdfstore-data-value").text();
            if(kind === 'uri') {
                this.application.showUriDialogModel.create(this, value);
                this.application.showUriDialogModel.application = this.application;
                this.application.showUriDialogModel.store = this.store;

            } else if(kind === 'literal') {
                var lang = jQuery(event.currentTarget).find("span.rdfstore-data-lang").text();
                var type = jQuery(event.currentTarget).find("span.rdfstore-data-type").text();

                this.application.showLiteralDialogModel.create(this, value, lang, type);
                this.application.showLiteralDialogModel.application = this.application;
                this.application.showLiteralDialogModel.store = this.store;

            } else if(kind === 'blank') {

            } else {
                // wtf?
            }
        },
    };

    // parsers
    ClientFrontend.rdfaParser = RDFaParser;
    ClientFrontend.rdfParser = RDFParser;

    window['client_frontend'] = ClientFrontend;
})();
