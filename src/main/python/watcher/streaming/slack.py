from dataclasses import dataclass

import pandas as pd

from .field_map import field_map_dict, attr_field_map_dict


@dataclass
class Slack:
    date: str = None,
    total_enabled_membership: int = None,
    enabled_full_members: int = None,
    enabled_guests: int = None,
    daily_active_members: int = None,
    daily_members_posting_messages: int = None,
    weekly_active_members: int = None,
    weekly_members_posting_messages: int = None,
    messages_in_public_channels: int = None,
    messages_in_private_channels: int = None,
    messages_in_shared_channels: int = None,
    messages_in_dms: int = None,
    percent_of_messages_public_channels: float = None,
    percent_of_messages_private_channels: float = None,
    percent_of_messages_dms: float = None,
    percent_of_views_public_channels: float = None,
    percent_of_views_private_channels: float = None,
    percent_of_views_dms: float = None,
    total_full_members: int = None,
    total_guests: int = None,
    total_claimed_full_members: int = None,
    total_claimed_guests: int = None,
    total_members: int = None,
    total_claimed_members: int = None,
    files_uploaded: int = None,
    messages_posted_by_members: int = None,
    name: int = None,
    public_channels_single_workspace: int = None,
    messages_posted: int = None,
    messages_posted_by_apps: int = None,

    def to_dict(self):
        return {"Date": self.date,
                "Total_Enabled_Membership": self.total_enabled_membership,
                "Enabled_Full_Members": self.enabled_full_members,
                "Enabled_Guests": self.enabled_guests,
                "Daily_active_members": self.daily_active_members,
                "Daily_members_posting_messages": self.daily_members_posting_messages,
                "Weekly_active_members": self.weekly_active_members,
                "Weekly_members_posting_messages": self.weekly_members_posting_messages,
                "Messages_in_public_channels": self.messages_in_public_channels,
                "Messages_in_private_channels": self.messages_in_private_channels,
                "Messages_in_shared_channels": self.messages_in_shared_channels,
                "Messages_in_DMs": self.messages_in_dms,
                "Percent_of_messages_public_channels": self.percent_of_messages_public_channels,
                "Percent_of_messages_private_channels": self.percent_of_messages_private_channels,
                "Percent_of_messages_DMs": self.percent_of_messages_dms,
                "Percent_of_views_public_channels": self.percent_of_views_public_channels,
                "Percent_of_views_private_channels": self.percent_of_views_private_channels,
                "Percent_of_views_DMs": self.percent_of_views_dms,
                "Total_Full_Members": self.total_full_members,
                "Total_Guests": self.total_guests,
                "Total_Claimed_Full_Members": self.total_claimed_full_members,
                "Total_Claimed_Guests": self.total_claimed_guests,
                "Total_Members": self.total_members,
                "Total_Claimed_Members": self.total_claimed_members,
                "Files_uploaded": self.files_uploaded,
                "Messages_posted_by_members": self.messages_posted_by_members,
                "Name": self.name,
                "Public_channels_single_workspace": self.public_channels_single_workspace,
                "Messages_posted": self.messages_posted,
                "Messages_posted_by_apps": self.messages_posted_by_apps}

    @classmethod
    def from_dict(cls, record: dict):
        return Slack(date=record.get("Date"),
                     total_enabled_membership=record.get("Total_Enabled_Membership"),
                     enabled_full_members=record.get("Enabled_Full_Members"),
                     enabled_guests=record.get("Enabled_Guests"),
                     daily_active_members=record.get("Daily_active_members"),
                     daily_members_posting_messages=record.get("Daily_members_posting_messages"),
                     weekly_active_members=record.get("Weekly_active_members"),
                     weekly_members_posting_messages=record.get("Weekly_members_posting_messages"),
                     messages_in_public_channels=record.get("Messages_in_public_channels"),
                     messages_in_private_channels=record.get("Messages_in_private_channels"),
                     messages_in_shared_channels=record.get("Messages_in_shared_channels"),
                     messages_in_dms=record.get("Messages_in_DMs"),
                     percent_of_messages_public_channels=record.get("Percent_of_messages_public_channels"),
                     percent_of_messages_private_channels=record.get("Percent_of_messages_private_channels"),
                     percent_of_messages_dms=record.get("Percent_of_messages_DMs"),
                     percent_of_views_public_channels=record.get("Percent_of_views_public_channels"),
                     percent_of_views_private_channels=record.get("Percent_of_views_private_channels"),
                     percent_of_views_dms=record.get("Percent_of_views_DMs"),
                     total_full_members=record.get("Total_Full_Members"),
                     total_guests=record.get("Total_Guests"),
                     total_claimed_full_members=record.get("Total_Claimed_Full_Members"),
                     total_claimed_guests=record.get("Total_Claimed_Guests"),
                     total_members=record.get("Total_Members"),
                     total_claimed_members=record.get("Total_Claimed_Members"),
                     files_uploaded=record.get("Files_uploaded"),
                     messages_posted_by_members=record.get("Messages_posted_by_members"),
                     name=record.get("Name"),
                     public_channels_single_workspace=record.get("Public_channels_single_workspace"),
                     messages_posted=record.get("Messages_posted"),
                     messages_posted_by_apps=record.get("Messages_posted_by_apps"))

    @classmethod
    def to_dict_list(cls, file_path: str) -> list:
        try:
            df = pd.read_csv(file_path)
            if not df.empty:
                print(f"File has data")
                return df.rename(columns=field_map_dict).to_dict("records")
        except pd.errors.EmptyDataError as ede:
            print(f"EmptyDataError '{ede}'")
