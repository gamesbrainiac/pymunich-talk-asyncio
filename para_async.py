# encoding=utf-8
# para_async.py
import asyncio
import itertools

import aiohttp


async def download(url, parts):

    # We have an overarching session object that manages
    # the GET and HEAD requests that are in the
    # function. In previous versions, we could
    # just use aiohttp.get but now we need to
    # create a separate session. session
    # is not just a session, its a
    # session pool.
    async with aiohttp.ClientSession() as session:

        # This handles getting the partial content with
        # start and end delimiters.
        async def get_partial_content(u, i, start, end):

            # For debugging, just to show you
            # which parts of the download
            # is being processed
            # print(i, start, end)

            # Since we are getting partial content, we
            # need to set the range of bytes that
            # we are dealing with.
            headers = {
                "Range": "bytes={}-{}".format(start, end - 1 if end else "")
            }

            async with session.get(u, headers=headers) as _resp:

                # The await servers to tell python that now it can
                # do something else, it is essentially another
                # `yield from`.
                return i, await _resp.read()

        async with session.head(url) as resp:
            size = int(resp.headers["Content-Length"])

        ranges = list(range(0, size, size // parts))

        things_to_wait_for = []
        for i, (start, end) in enumerate(
                itertools.zip_longest(ranges, ranges[1:], fillvalue='')):
            things_to_wait_for.append(get_partial_content(url, i, start, end))

        # res, _ = await asyncio.wait(
        #     [get_partial_content(url, i, start, end) for i, (start, end) in
        #      enumerate(
        #          itertools.zip_longest(ranges, ranges[1:], fillvalue=""))])

        res, _ = await asyncio.wait(things_to_wait_for)

        sorted_result = sorted(task.result() for task in res)
        return b"".join(data for _, data in sorted_result)


if __name__ == '__main__':
    image_url = "http://eoimages.gsfc.nasa.gov/" \
                "images/imagerecords/73000/73751/" \
                "world.topo.bathy.200407.3x21600x21600.D1.jpg"
    loop = asyncio.get_event_loop()
    bs = loop.run_until_complete(download(image_url, 16))

    print(len(bs))
    with open("test_para_async.jpeg", "wb") as fi:
        fi.write(bs)
