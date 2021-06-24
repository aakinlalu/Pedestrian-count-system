import json
from calendar import month_name
from dataclasses import dataclass
from datetime import date
from typing import List, Tuple

import aiobotocore
import aiofiles
import aiohttp
from eliot import start_action

from configs.configs import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
from logger.logger import __LOGGER__

__LOGGER__


@dataclass
class APIUrl:
    url: str
    bucket: str
    dir_name: str
    AWS_ACCESS_KEY_ID: str = AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY: str = AWS_SECRET_ACCESS_KEY

    @staticmethod
    def select_month(month_number: int) -> Tuple[str, int]:
        """
        Given the function any number of the month, from 1 to 12, it would
        return the current year and the name of the month. If no argument is
        provided to the function, the function would return current year and month
        name of the previous month if today date is 25 or more, otherwise it would
        return current year and None.

        >> select_month(2)
           (2020, February)

        :param month_number
        :return A tuple of current year and month name of the user selection
        """

        month = None
        today = date.today()

        if month_number is None:
            if today.day >= 25:
                mm = today.month - 1
                if mm == 1:
                    month = month_name[12]
                else:
                    month = month_name[mm]
        else:
            if month_number in range(1, 13):

                month = month_name[month_number]

        return today.year, month

    async def fetch_url_payload(
        self, year: int = None, month: str = None
    ) -> List[dict]:
        """
        Fetch Pedestrian count system data using api endpoint
        >> asyncio.run(fetch_url_payload(2021, 'May))
             [{...},{...}]
        :param year: Year filters out data for the year. It is default to None.
        :param month: It filters out data specific for the month specified. It is default to None.
        :return: The list of dictionary
        """

        link = self.url
        if year is not None:
            link = f"{self.url}?year={year}"

        if all([year is not None, month is not None]):
            link = f"{self.url}?year={year}&month={month}"

        with start_action(action_type="capture api link", link=link) as action:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(link) as response:
                        response.raise_for_status()

                        action.log(message_type="info", status=response.status)

                        data = await response.json()

                        return data
            except Exception as e:
                raise ValueError(str(e))

    async def write_json_to_local(self, year: int = None, month: str = None) -> None:
        """
        Write payload as json to the current directory

        >> asyncio.run(write_json_to_local(2021, 'May))

        :param year: Year filters out data for the year. It is default to None.
        :param month: It filters out data specific for the month specified. It is default to None.
        :return: None
        """
        file_path = f"{self.dir_name}/file.json"

        if year is not None:
            file_path = f"{self.dir_name}/{year}_file.json"

        with start_action(
            action_type="write_json_to_local", file_path=file_path
        ) as action:
            try:
                result = await self.fetch_url_payload(year, month)
                if not result:
                    return None

                action.log(message_type="info", number_of_rows=len(result))

                async with aiofiles.open(file_path, mode="w") as f:
                    await f.write(json.dumps(result))
            except Exception as e:
                raise ValueError(str(e))

    async def write_json_to_s3(self, year: int = None, month: str = None) -> None:
        """
        Write payload as json to s3 bucket.

        >> asyncio.run(wite_json_to_s3(2021, 'May'))

        :param year: Year filters out data for the year. It is default to None.
        :param month: It filters out data specific for the month specified. It is default to None.
        :return: None
        """
        result = await self.fetch_url_payload(year, month)
        if not result:
            return None

        file_path = f"{self.dir_name}/file.json"
        if year is not None:
            file_path = f"{self.dir_name}/{year}_file.json"

        with start_action(
            action_type="write_json_to_s3", file_path=file_path
        ) as action:

            session = aiobotocore.get_session()
            async with session.create_client(
                "s3",
                aws_access_key_id=self.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            ) as s3:
                try:
                    resp = await s3.put_object(
                        Bucket=self.bucket, Key=file_path, Body=json.dumps(result)
                    )
                    action.log(message_type="info", length=len(result))
                    # print(resp)
                except Exception as e:
                    raise ValueError(str(e))
