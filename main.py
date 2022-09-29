from bing.pipeline import pipelines
from bing.pipeline_service import pipeline_service


def main(request):
    data: dict = request.get_json()
    print(data)

    if "table" in data:
        response = pipeline_service(
            pipelines[data["table"]],
            data["account_id"],
            data.get("start"),
            data.get("end"),
        )
    else:
        raise ValueError(data)

    print(response)
    return response
