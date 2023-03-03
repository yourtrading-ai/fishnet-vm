import asyncio
import logging
from pathlib import Path
from typing import List, Optional

from semver import VersionInfo

from aleph_client import AuthenticatedUserSession
from aleph_client.types import StorageEnum
from aleph_client.utils import create_archive
from aleph_message.models import ProgramMessage, StoreMessage, MessageType

from .discovery import discover_executors
from .constants import FISHNET_DEPLOYMENT_CHANNEL

import fishnet_cod

logger = logging.getLogger(__name__)


async def deploy_executors(
    executor_path: Path,
    time_slices: List[int],
    deployer_session: AuthenticatedUserSession,
    channel: str = FISHNET_DEPLOYMENT_CHANNEL,
):
    # Discover existing executor VMs
    executor_messages = await discover_executors(
        deployer_session.account.get_address(), deployer_session, channel
    )
    source_code_refs = set(
        [executor.content.code.ref for executor in executor_messages]
    )

    # Get latest version executors and source code
    with deployer_session:
        source_messages = await deployer_session.get_messages(
            hashes=source_code_refs, message_type=MessageType.store
        )
    latest_source: Optional[StoreMessage] = None
    for source in source_messages.messages:
        assert (
            source.content.service_version
        ), "[PANIC] Encountered source_code message with no version!"
        if not latest_source:
            latest_source = source
        elif VersionInfo.parse(source.content.service_version) > VersionInfo.parse(
            latest_source.content.service_version
        ):
            latest_source = source
    latest_executors = [
        executor
        for executor in executor_messages
        if executor.content.code.ref == latest_source.item_hash
    ]

    # Create new source archive from local files and hash it
    path_object, encoding = create_archive(executor_path)
    # TODO: Move file hashing methods to aleph-sdk-python
    # TODO: Compare hash with all past versions' content.item_hashes
    # If any are equal, throw error because of repeated deployment
    # TODO: Check version manifest whether this was intended

    # Upload the source code with new version
    with open(path_object, "rb") as fd:
        logger.debug("Reading file")
        # TODO: Read in lazy mode instead of copying everything in memory
        file_content = fd.read()
        storage_engine = (
            StorageEnum.ipfs
            if len(file_content) > 4 * 1024 * 1024
            else StorageEnum.storage
        )
        logger.debug("Uploading source file")
        with deployer_session as session:
            user_code, status = session.create_store(
                file_content=file_content,
                storage_engine=storage_engine,
                channel=FISHNET_DEPLOYMENT_CHANNEL,
                guess_mime_type=True,
                extra_fields={
                    "type": "executor",
                    "service_version": "",
                    "protocol_version": fishnet_cod.__version__,
                },
            )
        logger.debug("Upload finished")
        program_ref = user_code.item_hash
    # Register the program
    # TODO: Distinguish immutable upload and mutable update
    with deployer_session:
        message, status = deployer_session.create_program(
            program_ref=program_ref,
            entrypoint=entrypoint,
            runtime=runtime,
            storage_engine=StorageEnum.storage,
            channel=channel,
            memory=memory,
            vcpus=vcpus,
            timeout_seconds=timeout_seconds,
            persistent=persistent,
            encoding=encoding,
            volumes=volumes,
            subscriptions=subscriptions,
        )
    logger.debug("Upload finished")
    if print_messages or print_program_message:
        typer.echo(f"{message.json(indent=4)}")

    hash: str = message.item_hash
    hash_base32 = b32encode(b16decode(hash.upper())).strip(b"=").lower().decode()

    typer.echo(
        f"Your program has been uploaded on Aleph .\n\n"
        "Available on:\n"
        f"  {settings.VM_URL_PATH.format(hash=hash)}\n"
        f"  {settings.VM_URL_HOST.format(hash_base32=hash_base32)}\n"
        "Visualise on:\n  https://explorer.aleph.im/address/"
        f"{message.chain}/{message.sender}/message/PROGRAM/{hash}\n"
    )
