import logging

import sage.http_server.responses as responses

from traceback import format_exc
from uvloop import EventLoopPolicy
from asyncio import set_event_loop_policy
from os import environ
from sys import setrecursionlimit
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlunparse
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse, Response, StreamingResponse

from sage.database.core.dataset import Dataset
from sage.database.descriptors import VoidDescriptor, many_void
from sage.query_engine.types import QueryContext, Mappings
from sage.query_engine.sage_engine import SageEngine
from sage.database.saved_plan.saved_plan_manager_factory import SavedPlanManagerFactory


class SagePostQuery(BaseModel):
    """
    Data model for the body of POST SPARQL queries
    """
    query: str = Field(
        ..., description="The SPARQL query to execute.")
    defaultGraph: str = Field(
        ..., description="The URI of the default RDF graph queried.")
    next: str = Field(
        None, description=(
            "(Optional) A next link used to resume query execution from "
            "a saved state."))
    quota: int = Field(
        None, description="The duration of a quantum.")
    forceOrder: bool = Field(
        False, description="True to fix the join order, False otherwise.")
    topkStrategy: str = Field(
        "topk_server", description=(
            "The strategy used to compute TOP-K queries. "
            "It can be 'topk_server' or 'partial_topk'."))
    earlyPruning: bool = Field(
        False, description="True to enable early pruning, False otherwise.")


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


async def execute_query(
    query: str, default_graph_uri: str, next_link: Optional[str],
    context: QueryContext = {}
) -> Tuple[List[Mappings], Optional[str], Dict[str, str]]:
    """
    Executes a query using the SageEngine and returns the appropriate HTTP
    response.

    Parameters
    ----------
        query: str
            SPARQL query to execute.
        default_graph_uri: str
            URI of the default RDF graph to use.
        next_link: None | str
            The saved plan of the query. Can be an identifier if the saved plan
            is stored on the server. Can be None if the execution of the query
            should starts from the beginning.
        context: QueryContext
            Global variables specific to the execution of the query.

    Returns
    -------
    Tuple[List[Mappings], Optional[str], Dict[str, Any]]
        A tuple (solutions, next_link, statistics) where:
            - solutions: the solutions found during query execution.
            - next_link: the saved physical execution plan. Can be None if
                query execution completed during the time quantum.
            - statistics: statistics collected during query execution.

    Raises
    ------
    Any exception that have occured during query execution.
    """
    try:
        saved_plan_manager = SavedPlanManagerFactory.create()
        if next_link is not None:
            saved_plan = saved_plan_manager.get_plan(next_link)
        else:
            saved_plan = None

        engine = SageEngine()
        solutions, saved_plan, statistics = await engine.execute(
            query, saved_plan, default_graph_uri, context=context)

        if saved_plan is not None:
            next_link = saved_plan_manager.save_plan(saved_plan)
        else:
            next_link = None

        return solutions, next_link, statistics
    except Exception as error:
        logging.error(format_exc())  # print the stacktrace
        raise error


def create_response(
    mimetypes: List[str], solutions: List[Mappings],
    next_page: Optional[str], stats: dict, skol_url: str
) -> Response:
    """
    Creates an HTTP response to return the result of a SPARQL query.

    Parameters
    ----------
    mimetypes: List[str]
        Mimetypes from the input HTTP request.
    solutions: List[Dict[str, str]]
        List of query results.
    next_link: None | str
        Link to a SaGe saved plan. Use `None` if there is no one, i.e., the
        query execution has completed during the quantum.
    stats: Dict[str, Any]
        Statistics about query execution.
    skol_url: str
        URL used for the skolemization of blank nodes.

    Returns
    -------
    Response
        An HTTP response built from the input mimetypes and the SPARQL
        query results.
    """
    if "application/json" in mimetypes:
        iterator = responses.raw_json_streaming(solutions, next_page, stats, skol_url)
        return StreamingResponse(iterator, media_type="application/json")
    elif "application/sparql-results+json" in mimetypes:
        iterator = responses.w3c_json_streaming(solutions, next_page, stats, skol_url)
        return StreamingResponse(iterator, media_type="application/json")
    elif "application/xml" in mimetypes or "application/sparql-results+xml" in mimetypes:
        iterator = responses.w3c_xml(solutions, next_page, stats)
        return Response(iterator, media_type="application/xml")
    return JSONResponse({"bindings": solutions, "next": next_page, "stats": stats})


def run_app() -> FastAPI:
    """
    Creates the HTTP server, compatible with uvicorn/gunicorn.

    Returns
    -------
    FastAPI
        The FastAPI HTTP application.
    """
    # enable uvloop for SPARQL query processing
    set_event_loop_policy(EventLoopPolicy())
    # set recursion depth (due to pyparsing issues)
    setrecursionlimit(3000)
    # create the HTTP server & activate CORS
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"])

    @app.get("/")
    async def root():
        return "The SaGe SPARQL query server is running!"

    @app.get("/sparql")
    async def sparql_get(
        request: Request,
        query: str = Query(
            ..., description="The SPARQL query to execute."),
        default_graph_uri: str = Query(
            ..., alias="default-graph-uri", description=(
                "The URI of the default RDF graph queried.")),
        next_link: str = Query(
            None, alias="next", description=(
                "(Optional) A next link used to resume query execution "
                "from a saved state."))
    ) -> Response:
        try:
            mimetypes = request.headers["accept"].split(",")
            server_url = urlunparse(request.url.components[0:3] + (None, None, None))
            solutions, next_page, stats = await execute_query(
                query, default_graph_uri, next_link)
            return create_response(mimetypes, solutions, next_page, stats, server_url)
        except HTTPException as error:
            raise error
        except Exception as error:
            raise HTTPException(status_code=500, detail=str(error))

    @app.post("/sparql")
    async def sparql_post(request: Request, item: SagePostQuery) -> Response:
        context = {}
        if item.quota is not None:
            context["quota"] = item.quota
        if item.forceOrder is not None:
            context["force_order"] = item.forceOrder
        if item.topkStrategy is not None:
            context["topk_strategy"] = item.topkStrategy
        if item.earlyPruning is not None:
            context["early_pruning"] = item.earlyPruning
        try:
            mimetypes = request.headers["accept"].split(",")
            server_url = urlunparse(request.url.components[0:3] + (None, None, None))
            solutions, next_page, stats = await execute_query(
                item.query, item.defaultGraph, item.next, context=context)
            return create_response(mimetypes, solutions, next_page, stats, server_url)
        except HTTPException as error:
            raise error
        except Exception as error:
            raise HTTPException(status_code=500, detail=str(error))

    @app.get("/void/", description="Get the VoID description of the SaGe server")
    async def server_void(request: Request) -> Response:
        """
        Describes all RDF datasets hosted by the Sage endpoint.
        """
        try:
            mimetypes = request.headers["accept"].split(",")
            url = urlunparse(request.url.components[0:3] + (None, None, None))
            if url.endswith("/"):
                url = url[0:len(url) - 1]
            void_format, res_mimetype = choose_void_format(mimetypes)
            description = many_void(url, void_format)
            return Response(description, media_type=res_mimetype)
        except Exception as err:
            logging.error(err)
            raise HTTPException(status_code=500, detail=str(err))

    @app.get("/.well-known/void/")
    async def well_known() -> RedirectResponse:
        return RedirectResponse(url="/void/")

    @app.get(
        "/void/{graph_name}",
        description="Get the VoID description of a RDF Graph hosted by the SaGe server")
    async def graph_void(
        request: Request, graph_name: str = Field(..., description="Name of the RDF Graph")
    ) -> Response:
        """
        Returns the VoID description of a RDF Graph hosted by the SaGe server.
        """
        dataset = Dataset()
        if not dataset.has_graph(graph_name) is None:
            raise HTTPException(
                status_code=404,
                detail=f"RDF Graph {graph_name} not found on the server.")
        try:
            mimetypes = request.headers["accept"].split(",")
            url = urlunparse(request.url.components[0:3] + (None, None, None))
            if url.endswith("/"):
                url = url[0:len(url) - 1]
            descriptor = VoidDescriptor(url, dataset.get_graph(graph_name))
            void_format, res_mimetype = choose_void_format(mimetypes)
            return Response(descriptor.describe(void_format), media_type=res_mimetype)
        except Exception as error:
            logging.error(error)
            raise HTTPException(status_code=500, detail=str(error))

    return app


if __name__ == "__main__":
    raise RuntimeError(
        "You cannot run the script server.py as a plain script. "
        "Please use the SaGe CLI to start you own server.")
elif "SAGE_CONFIG_FILE" not in environ:
    raise Exception("No YAML configuration file provided...")

app = run_app()
