import datetime

from peopleai_api import PeopleAIClient


def main():
    client = PeopleAIClient("Jl83bbH73WUu", "CsxvdFp92EgIMxA7UOK7RzsatvrhjQGdP3HV2nthasgAHZIz")

    start_date = "2022-07-17"
    end_date = "2022-07-17"

    param_dict = {
        "start_date": start_date,
        "end_date": end_date,
        "activity_type": "all",
        "output_format": "JSONLines",
        "export_type": "delta"
    }

    started_time = datetime.datetime.now()
    print(f"Starting a job from '{start_date}' to '{end_date}'")
    job_id = client.start_activities_export(**param_dict)
    # job_id = 976511

    print(f"Polling for the job '{job_id}' to complete")
    client.check_activities_export(job_id, until_completed=True)

    print(f"Exporting data to JSON file")
    client.download_activities_export(job_id,
                                      f"peopleai-export-{start_date.replace('-', '')}-{end_date.replace('-', '')}.json")

    finished_time = datetime.datetime.now()
    print(f"Job started at '{started_time}' and finished at '{finished_time}'")


if __name__ == "__main__":
    main()
