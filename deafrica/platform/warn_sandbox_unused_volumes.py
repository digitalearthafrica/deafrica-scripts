import boto3
import click as click
from datetime import datetime
from datetime import timedelta
# from kubernetes import config, client
import os

# PDS
# ACCESS_KEY="AKIAX5HBFNR6TAMMGBUR"
# SECRET_KEY="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"

# DEV
# ACCESS_KEY = "AKIA2OGNIYV6SUU35AGA"
# SECRET_KEY = "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"

# PROD
ACCESS_KEY="AKIAYHJMCO7PBQFK6XMY"
SECRET_KEY="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"

def get_all_users():
        
    users = []
    next_page = None
    kwargs = {
        'UserPoolId': "us-west-2_v9nJrst3o"
    }

    users_remain = True
    while users_remain:
        if next_page:
            kwargs['PaginationToken'] = next_page
        response = cognito_client.list_users(**kwargs)
        users.extend(response['Users'])
        next_page = response.get('PaginationToken', None)
        users_remain = next_page is not None

    return users

def WarnTests():

    os.environ["AWS_ACCESS_KEY_ID"] = ACCESS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = SECRET_KEY
    os.environ["AWS_DEFAULT_REGION"] = "af-south-1"

    #configure boto3 client
    ec2_resource = boto3.resource("ec2")
    ec2_client = boto3.client('ec2')
    ct_client = boto3.client("cloudtrail")
    ses_client = boto3.client('ses')

    cluster_name="deafrica-prod-af-eks"
    k8s_namespace = "sandbox"
    daysback = 90

    time_now = datetime.now()
    time_back = time_now - timedelta(days=daysback)
    filters = [
        {
            "Name": f"tag:kubernetes.io/cluster/{cluster_name}",
            "Values": [
                "owned",
            ],
        },
        {
            "Name": "tag:kubernetes.io/created-for/pvc/namespace",
            "Values": [
                k8s_namespace,
            ],
        },
    ]
    count = 0

    volume_ids_warn = []
    last_logged_in = []

    for volume in ec2_resource.volumes.filter(Filters=filters):
        response = ct_client.lookup_events(
            LookupAttributes=[
                {"AttributeKey": "ResourceName", "AttributeValue": volume.id},
            ],
            MaxResults=1000,
            StartTime=time_back,
            EndTime=time_now,
        )

        attach_events = [
            event
            for event in response["Events"]
            if event.get("EventName") == "AttachVolume"
        ]
        
        if len(attach_events)>0:
            tz_info = attach_events[0]["EventTime"].tzinfo
            last_logged_in.append(datetime.now(tz_info)-attach_events[0]["EventTime"])
        else:
            last_logged_in.append(timedelta(days=100))
        
        if len(attach_events) == 0 and volume.state == "available":
            volume_ids_warn.append(volume.id)
            
    print(volume_ids_warn)

    print('30 to 60 days unused:')
    for i in last_logged_in:
        if (i>timedelta(days=30)) and (i<timedelta(days=60)):
            print(i, ' Time Left: ',timedelta(days=90)-i)

    print('60 to 90 days unused:')
    for i in last_logged_in:
        if (i>timedelta(days=60)) and (i<timedelta(days=90)):
            print(i, ' Time Left: ',timedelta(days=90)-i)

    volume_desc=ec2_client.describe_volumes(VolumeIds=volume_ids_warn)
    # print(volume_desc)

    claims_warn = []

    for volume in volume_desc["Volumes"]:
        for tag in volume["Tags"]:
            if tag["Key"]=="kubernetes.io/created-for/pvc/name":
                claims_warn.append(tag["Value"][6:])
                
    print(claims_warn)
            
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
    cognito_client = boto3.client("cognito-idp")

    
            
    all_users=get_all_users()
    print(all_users[0])
    print(len(all_users))

    claims_warn_mod = []

    for claim in claims_warn:
        claim1=claim.replace("-40","@")
        claim2=claim1.replace("-2e",".")
        claims_warn_mod.append(claim2)
        
    print(claims_warn_mod)

    users_warn = []
    email_warn = []

    for user in all_users:
        email_address = "NULL"
        for attribute in user["Attributes"]:
            if attribute["Name"]=="email":
                email_address=attribute["Value"]
        if email_address in claims_warn_mod:
            for attribute in user["Attributes"]:
                if attribute["Name"]=="name":
                    users_warn.append(attribute["Value"])
                    email_warn.append(email_address)
                    
    print(users_warn)
    print(email_warn)
                    
        
    # for instance in ec2_resource.instances.all():
    #      print(
    #          "Id: {0}\nPlatform: {1}\nType: {2}\nPublic IPv4: {3}\nAMI: {4}\nState: {5}\n".format(
    #          instance.id, instance.platform, instance.instance_type, instance.public_ip_address, instance.image.id, instance.state
    #          )
    #      )

    SMTP_SECRET = "BImXMTqWUCAx2u0ru/yGiwwmY9bzQ1QKplWk+9KK4xZl"

    ses_send_quota = ses_client.get_send_quota()
    print(ses_send_quota)

    # verification_response = ses_client.get_identity_verification_attributes(
    #     Identities=[
    #         'schowdhury@sansa.org.za',
    #     ]
    # )
    # print(verification_response)


    # response = ses_client.send_email(
    #     Destination={
    #         'BccAddresses': [
    #             'schowdhury@sansa.org.za',
    #             'kmithi@sansa.org.za',
    #             'leon@kartoza.com',
    #             'bhaskar@kartoza.com'
    #         ],
    #         'CcAddresses': [
    #         ],
    #         'ToAddresses': email_warn
    #     },
    #     Message={
    #         'Body': {
    #             'Html': {
    #                 'Charset': 'utf-8',
    #                 'Data': 'Good Day. \n Your Digital Earth Africa Sandbox Volume will be deleted in 30 days! Please login to your DE Africa Sandbox to prevent this from happening.',
    #             },
    #             'Text': {
    #                 'Charset': 'utf-8',
    #                 'Data': 'Good Day. \n Your Digital Earth Africa Sandbox Volume will be deleted in 30 days! Please login to your DE Africa Sandbox to prevent this from happening.',
    #             },
    #         },
    #         'Subject': {
    #             'Charset': 'utf-8',
    #             'Data': 'Warning - Your Digital Earth Africa Sandbox Volume will be deleted in 30 days!',
    #         },
    #     },
    #     Source='systems@digitalearthafrica.org'
    # )

    # print(response)

@click.command("warn-unused-volumes")
def cli():
    """
    Warn sandbox unused volume owners via email
    """
    WarnTests()