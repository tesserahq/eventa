import logging
from app.middleware.db_session import DBSessionMiddleware
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import get_settings
import rollbar
from rollbar.logger import RollbarHandler
from rollbar.contrib.fastapi import ReporterMiddleware as RollbarMiddleware
from fastapi_pagination import add_pagination

from .routers import (
    event,
    user,
)
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from app.telemetry import setup_tracing
from app.exceptions.handlers import register_exception_handlers
from app.core.logging_config import get_logger
from app.ext.nats_subscriber import NatsSubscriber
from app.events.handlers import EVENT_SUBSCRIPTIONS

SKIP_PATHS = ["/gazettes/share/", "/health", "/openapi.json", "/docs"]


def create_app(testing: bool = False, auth_middleware=None) -> FastAPI:
    logger = get_logger()
    settings = get_settings()

    subscriber: NatsSubscriber | None = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal subscriber
        async with mcp_app.lifespan(app):
            if not testing and settings.nats_subscriber_enabled:
                logger.info("Main: Starting NATS subscriber")
                try:
                    subscriber = NatsSubscriber(
                        servers=settings.nats_servers,
                        client_name=settings.nats_client_id,
                        client_secret=settings.nats_client_secret,
                        stream_name=settings.nats_stream_name,
                        durable_name=settings.nats_consumer_name,
                    )
                    app.state.nats_subscriber = subscriber
                    for subscription in EVENT_SUBSCRIPTIONS:
                        try:
                            await subscriber.subscribe(
                                subject=subscription.subject,
                                handler=subscription.handler,
                                queue=subscription.queue or settings.nats_queue_group,
                                auto_ack=subscription.auto_ack,
                                stream=subscription.stream or settings.nats_stream_name,
                            )
                            logger.info(
                                "Main: Subscribed to subject '%s' (queue=%s)",
                                subscription.subject,
                                subscription.queue or settings.nats_queue_group,
                            )
                        except Exception as exc:
                            logger.error(
                                "Main: Failed to subscribe to NATS subject '%s'",
                                subscription.subject,
                                exc_info=exc,
                            )
                except Exception as exc:
                    logger.error(
                        "Main: Unable to initialize NATS subscriber", exc_info=exc
                    )
                    subscriber = None
                    app.state.nats_subscriber = None
            try:
                yield
            finally:
                if subscriber is not None:
                    logger.info("Main: Closing NATS subscriber")
                    try:
                        await subscriber.close()
                    except Exception as exc:
                        logger.error(
                            "Main: Error while closing NATS subscriber", exc_info=exc
                        )
                    finally:
                        app.state.nats_subscriber = None
                        subscriber = None
                else:
                    app.state.nats_subscriber = None

    app = FastAPI(lifespan=lifespan)
    
    if settings.is_production:
        # Initialize Rollbar SDK with your server-side access token
        rollbar.init(
            settings.rollbar_access_token,
            environment=settings.environment,
            handler="async",
        )

        # Report ERROR and above to Rollbar
        rollbar_handler = RollbarHandler()
        rollbar_handler.setLevel(logging.ERROR)

        # Attach Rollbar handler to the root logger
        logger.addHandler(rollbar_handler)
        app.add_middleware(RollbarMiddleware)

    if not testing and not settings.disable_auth:
        logger.info("Main: Adding authentication middleware")
        from tessera_sdk.middleware.authentication import AuthenticationMiddleware
        from tessera_sdk.middleware.user_onboarding import UserOnboardingMiddleware
        from tessera_sdk.utils.service_factory import create_service_factory
        from app.services.user_service import UserService

        # Create service factory for UserService
        user_service_factory = create_service_factory(UserService)

        app.add_middleware(
            UserOnboardingMiddleware,
            identies_base_url=settings.identies_host,
            user_service_factory=user_service_factory,
        )
        app.add_middleware(
            AuthenticationMiddleware,
            identies_base_url=settings.identies_host,
            skip_paths=SKIP_PATHS,
        )

    else:
        logger.info("Main: No authentication middleware")
        if auth_middleware:
            app.add_middleware(auth_middleware)

    app.add_middleware(DBSessionMiddleware)

    # TODO: Restrict this to the allowed origins
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Puedes restringir esto a dominios específicos
        allow_credentials=True,
        allow_methods=["*"],  # Permitir todos los métodos (GET, POST, etc.)
        allow_headers=["*"],  # Permitir todos los headers
    )

    app.include_router(event.router)
    app.include_router(user.router)

    register_exception_handlers(app)

    # Add pagination support
    add_pagination(app)

    return app


# Production app instance
app = create_app()

settings = get_settings()
if settings.otel_enabled:
    tracer_provider = setup_tracing()  # Or use env/config
    FastAPIInstrumentor.instrument_app(app, tracer_provider=tracer_provider)


@app.get("/")
def main_route():
    return {"message": "Hey, It is me Goku"}
