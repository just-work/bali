import asyncio

import pytest
from google.protobuf import message
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String

from bali.db import db
from bali.decorators import action
from bali.resources import ModelResource
from bali.schemas import ListRequest

DB_URI = 'sqlite:///:memory:'

db.connect(DB_URI)


class User(db.BaseModel):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(20), index=True)


class UserSchema(BaseModel):
    id: int
    username: str

    class Config:
        orm_mode = True


class UserResource(ModelResource):
    model = User
    schema = UserSchema
    filters = [
        {'username': str},
    ]

    @staticmethod
    async def generate_sequence():
        for _ in range(1):
            await asyncio.sleep(1)
            yield [User(username='test1', id=1)]

    @action()
    async def list(self, schema_in: ListRequest = None):
        return await self.generate_sequence().__anext__()


@pytest.fixture
def mock_request_data(mocker):
    return mocker.patch(
        "bali.decorators.MessageToDict"
    )


@pytest.fixture
def mock_response_data(mocker):
    return mocker.patch(
        "bali.decorators.ParseDict"
    )


class AsyncListService:
    """ Mock grpc service """
    async def UserList(self, request, context):
        response = message.Message
        return await UserResource(request, context, response).list()


@pytest.mark.asyncio
async def test_paginate(mock_request_data, mock_response_data):
    """ Paginate function handles correctly async rpc request.

    This test verifies that the paginate function in asynchronous rps mode
    doesn't throw a "Model instance can't parse to dict without schema" error
    when using a model schema.
    """
    mock_request_data.return_value = {'limit': 10, 'offset': 0}
    mock_response_data.return_value = {'count': 2}
    request = message.Message()
    service = AsyncListService()

    try:
        await service.UserList(request, {})
    except Exception as e:
        pytest.fail(str(e))
