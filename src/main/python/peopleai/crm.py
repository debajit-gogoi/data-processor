import json


def extract_crm_status_and_associate_matches(crm_status_json_keys, crm_matched_json_keys, crm_status_record,
                                             crm_status_parsed_responses, uid):
    crm_status_parsed_response = []

    crm_status_parsed_response.append(uid)
    for json_key in crm_status_json_keys:
        if json_key in crm_status_record:
            crm_status_parsed_response.append(crm_status_record[json_key])
        else:
            crm_status_parsed_response.append(None)

    if 'account' in crm_status_record:
        crm_account_record = crm_status_record['account']
        for acc_field in ['crm_id', 'name']:
            if acc_field in crm_account_record:
                crm_status_parsed_response.append(crm_account_record[acc_field])
            else:
                crm_status_parsed_response.append(None)
    else:
        crm_status_parsed_response.append(None)
        crm_status_parsed_response.append(None)

    if 'matched_to' in crm_status_record and crm_status_record['matched_to'] != 'none':
        matched_record = crm_status_record['matched_to']
        for json_key in crm_matched_json_keys:
            if matched_record in crm_status_record and json_key in crm_status_record[matched_record]:
                crm_status_parsed_response.append(crm_status_record[matched_record][json_key])
            else:
                crm_status_parsed_response.append(None)
    else:
        for json_key in crm_matched_json_keys:
            crm_status_parsed_response.append(None)

    crm_status_parsed_responses.append(tuple(crm_status_parsed_response))

    return tuple(crm_status_parsed_response)


def decode_people_ai_api_json_response(api_json_responses):
    crm_status_parsed_responses = []

    crm_status_json_keys = ['pushed', 'push_time', 'crm_id', 'matched_to']
    crm_matched_json_keys = ['crm_id', 'name', 'email', 'parent_account_crm_id', 'parent_account_name',
                             'stage_name', 'stage_rank']

    for record in api_json_responses:
        response_record = json.loads(record)
        uid = response_record['uid']

        matched_to = response_record['crm_status']['matched_to']
        if matched_to == "none":
            data = extract_crm_status_and_associate_matches(crm_status_json_keys, crm_matched_json_keys,
                                                     response_record['crm_status'],
                                                     crm_status_parsed_responses,
                                                     uid)
            print(f"response_record: '{response_record}'")
            print(f"data: {data}")
            print()

    print(f"crm_status_parsed_responses: {crm_status_parsed_responses}")


if __name__ == "__main__":
    data_list = None
    with open(
            "/main/python/peopleai/data/peopleai-export-20220715-20220715.json",
            mode="r", encoding="utf-8") as fp:
        data_list = fp.readlines()

    # print(f"data_str {data_list[0: 10]}")

    decode_people_ai_api_json_response(data_list)
