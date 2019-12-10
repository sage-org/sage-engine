# server.py
# Author: Thomas MINIER - MIT License 2017-2020
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response, StreamingResponse
from starlette.middleware.cors import CORSMiddleware
from sage.database.core.yaml_config import load_config
from sage.query_engine.sage_engine import SageEngine
from sage.query_engine.optimizer.query_parser import parse_query
from sage.query_engine.iterators.loader import load
from sage.http_server.utils import format_graph_uri, encode_saved_plan, decode_saved_plan
from sage.database.descriptors import VoidDescriptor, many_void
import sage.http_server.responses as responses
from urllib.parse import urlunparse
import logging
from time import time
from uuid import uuid4
from sys import setrecursionlimit


class SagePostQuery(BaseModel):
    """Data model for the body of POST SPARQL queries"""
    query: str = Field(..., description="The SPARQL query to execute.")
    defaultGraph: str = Field(..., description="The URI of the default RDF graph queried.")
    next: str = Field(None, description="(Optional) A next link used to resume query execution from a saved state.")

def choose_void_format(mimetypes):
    if "text/turtle" in mimetypes:
        return "turtle", "text/turtle"
    elif "application/xml" in mimetypes:
        return "xml", "application/xml"
    elif "application/n-quads" in mimetypes:
        return "nquads", "application/n-quads"
    elif "application/trig" in mimetypes:
        return "trig", "application/trig"
    elif "application/json" in mimetypes or "application/json+ld" in mimetypes:
        return "json-ld", "application/json"
    return "ntriples", "application/n-triples"

async def execute_query(query, default_graph_uri, next_link, dataset, url):
    """
        Execute a query using the SageEngine and returns the appropriate HTTP response.
        Any failure will results in a rollback/abort on the current query execution.
    """
    graph = None
    try:
        graph_name = format_graph_uri(default_graph_uri, url)
        if not dataset.has_graph(graph_name):
            raise HTTPException(status_code=404, detail=f"RDF Graph {graph_name} not found on the server.")
        graph = dataset.get_graph(graph_name)

        # decode next_link or build query execution plan
        cardinalities = dict()
        start = time()
        if next_link is not None:
            if dataset.is_stateless:
                saved_plan = next_link
            else:
                saved_plan = dataset.statefull_manager.get_plan(next_link)
            plan = load(decode_saved_plan(saved_plan), dataset)
        else:
            plan, cardinalities = parse_query(query, dataset, graph_name, url)
        loading_time = (time() - start) * 1000

        # execute query
        engine = SageEngine()
        quota = graph.quota / 1000
        max_results = graph.max_results
        bindings, saved_plan, is_done, abort_reason = await engine.execute(plan, quota, max_results)

        # commit or abort (if necessary)
        if abort_reason is not None:
            graph.abort()
            raise HTTPException(status_code=500, detail=f"The SPARQL query has been aborted for the following reason: '{abort_reason}'")
        else:
            graph.commit()

        start = time()
        # encode saved plan if query execution is not done yet and there was no abort
        next_page = None
        if (not is_done) and abort_reason is None:
            next_page = encode_saved_plan(saved_plan)
            if not dataset.is_stateless:
                # generate the plan ID if this is the first time we execute this plan
                plan_id = next_link if next_link is not None else str(uuid4())
                dataset.statefull_manager.save_plan(plan_id, next_page)
                next_page = plan_id
        elif is_done and (not dataset.is_stateless) and next_link is not None:
            # delete the saved plan, as it will not be reloaded anymore
            dataset.statefull_manager.delete_plan(next_link)

        exportTime = (time() - start) * 1000
        stats = {"cardinalities": cardinalities, "import": loading_time, "export": exportTime}

        return (bindings, next_page, stats)
    except Exception as err:
        # abort all ongoing transactions, then forward the exception to the main loop
        if graph is not None:
            graph.abort()
        raise err

def create_response(mimetypes, bindings, next_page, stats, url):
    """Create an HTTP response, given a set of mimetypes"""
    if "application/json" in mimetypes:
        iterator = responses.raw_json_streaming(bindings, next_page, stats, url)
        return StreamingResponse(iterator, media_type="application/json")
    elif "application/sparql-results+json" in mimetypes:
        iterator = responses.w3c_json_streaming(bindings, next_page, stats, url)
        return StreamingResponse(iterator, media_type="application/json")
    elif "application/xml" in mimetypes or "application/sparql-results+xml" in mimetypes:
        iterator = responses.w3c_xml(bindings, next_page, stats)
        return Response(iterator, media_type="application/xml")
    return JSONResponse({
        "bindings": bindings,
        "next": next_page,
        "stats": stats
    })

def run_app(config_file: str):
    """Create the HTTP server, compatible with uvicorn/gunicorn"""
     # set recursion depth (due to pyparsing issues)
    setrecursionlimit(3000)

    # create the HTTP server & activate CORS
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Build the RDF dataset from the configuration file
    dataset = load_config(config_file)


    @app.get("/")
    async def root():
        return "The SaGe SPARQL query server is running!"


    @app.get("/sparql")
    async def sparql_get(
        request: Request,
        query: str = Query(..., description="The SPARQL query to execute."),
        default_graph_uri: str = Query(..., alias="default-graph-uri", description="The URI of the default RDF graph queried."),
        next_link: str = Query(None, alias="next", description="(Optional) A next link used to resume query execution from a saved state.")
    ):
        """Execute a SPARQL query using the Web Preemption model"""
        try:
            mimetypes = request.headers['accept'].split(",")
            server_url = urlunparse(request.url.components[0:3] + (None, None, None))
            bindings, next_page, stats = await execute_query(query, default_graph_uri, next_link, dataset, server_url)
            return create_response(mimetypes, bindings, next_page, stats, server_url)
        except HTTPException as err:
            raise err
        except Exception as err:
            logging.error(err)
            raise HTTPException(status_code=500, detail=str(err))


    @app.post("/sparql")
    async def sparql_post(request: Request, item: SagePostQuery):
        """Execute a SPARQL query using the Web Preemption model"""
        try:
            mimetypes = request.headers['accept'].split(",")
            server_url = urlunparse(request.url.components[0:3] + (None, None, None))
            bindings, next_page, stats = await execute_query(item.query, item.defaultGraph, item.next, dataset, server_url)
            return create_response(mimetypes, bindings, next_page, stats, server_url)
        except HTTPException as err:
            raise err
        except Exception as err:
            logging.error(err)
            raise HTTPException(status_code=500, detail=str(err))
    
    @app.get("/void/", description="Get the VoID description of the SaGe server")
    async def server_void(request: Request):
        """Describe all RDF datasets hosted by the Sage endpoint"""
        try:
            mimetypes = request.headers['accept'].split(",")
            url = urlunparse(request.url.components[0:3] + (None, None, None))
            if url.endswith('/'):
                url = url[0:len(url) - 1]
            void_format, res_mimetype = choose_void_format(mimetypes)
            description = many_void(url, dataset, void_format)
            return Response(description, media_type=res_mimetype)
        except Exception as err:
            logging.error(err)
            raise HTTPException(status_code=500, detail=str(err))
    
    @app.get("/.well-known/void/")
    async def well_known():
        """Alias for /void/"""
        return RedirectResponse(url="/void/")
    
    @app.get("/void/{graph_name}", description="Get the VoID description of a RDF Graph hosted by the SaGe server")
    async def graph_void(request: Request, graph_name: str = Field(..., description="Name of the RDF Graph")):
        """Get the VoID description of a RDF Graph hosted by the SaGe server"""
        graph = dataset.get_graph(graph_name)
        if graph is None:
            raise HTTPException(status_code=404, detail=f"RDF Graph {graph_name} not found on the server.")
        try:
            mimetypes = request.headers['accept'].split(",")
            url = urlunparse(request.url.components[0:3] + (None, None, None))
            if url.endswith('/'):
                url = url[0:len(url) - 1]
            descriptor = VoidDescriptor(url, graph)
            void_format, res_mimetype = choose_void_format(mimetypes)
            return Response(descriptor.describe(void_format), media_type=res_mimetype)
        except Exception as err:
            logging.error(err)
            raise HTTPException(status_code=500, detail=str(err))
    
    return app
    