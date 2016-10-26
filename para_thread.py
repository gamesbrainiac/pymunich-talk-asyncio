# encoding=utf-8
import concurrent.futures

import itertools
import requests


def get_partial_content(u, i, start, end):
    # print(i, start, end)

    headers = {
        "Range": "bytes={}-{}".format(start, end - 1 if end else "")
    }

    resp = requests.get(u, headers=headers)
    return i, resp.content


def download(url, parts):

    resp = requests.head(url)
    size = int(resp.headers['Content-Length'])

    ranges = list(range(0, size, size // parts))

    with concurrent.futures.ThreadPoolExecutor(max_workers=parts) as executor:
        futures = []
        for i, (start, end) in enumerate(
                itertools.zip_longest(ranges, ranges[1:], fillvalue='')):
            future = executor.submit(get_partial_content, url, i, start, end)
            futures.append(future)

        results = sorted(
            future.result() for future in
            concurrent.futures.as_completed(futures))

        return b''.join(data for _, data in results)


if __name__ == '__main__':
    image_url = \
        "http://effigis.com/wp-content/uploads/" \
        "2015/02/Airbus_Pleiades_50cm_8bit_RGB_Yogyakarta.jpg"

    bs = download(image_url, 64)
    print(len(bs))
    with open('test_para_thread.jpeg', 'wb') as fi:
        fi.write(bs)
