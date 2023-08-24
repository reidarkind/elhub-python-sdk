"""
Masterdata related functions

"""
import logging
from typing import Tuple 
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

import zeep
from zeep.plugins import HistoryPlugin
from elhub_sdk.constants import ELHUB_GSN, TIME_FORMAT
from elhub_sdk.enums import (
    BSR_IDS,
    DOCUMENT_TYPE_UN_CEFACT,
    ENERGY_INDUSTRY_CLASSIFICATION,
    LIST_AGENCY_IDENTIFIER,
    QUERY_MARKET,
    ROLES,
    SCHEME_AGENCY_IDENTIFIER,
)

logger = logging.getLogger()


def _create_eh_request(
    query_type_code:QUERY_MARKET,
    client: zeep.Client, 
    meter_identificator: str,
    sender_gsn: str,
    snapshotdate: datetime=None, 
    start: datetime = None, 
    end: datetime = None,
    process_role: ROLES = ROLES.THIRD_PARTY,
):

    # create the generic part of the payload
    payload = {
        'QueryTypeCode': query_type_code.value,
        'MeteringPointUsedDomainLocation': {
            'Identification': {
                '_value_1': meter_identificator,
                'schemeAgencyIdentifier': SCHEME_AGENCY_IDENTIFIER.GS1.value,
                }
            }
        }
    # add SnapShotOccurence or Period
    if query_type_code.value == QUERY_MARKET.MASTERDATA_CUSTOMER.value:
        # must have SnapShotOccurence
        if snapshotdate is None:
            raise ValueError('Query type "MDCU" must have SnapShotOccurence. "snapshotdate" is None, should be datetime.')
        payload["SnapShotOccurrence"] = snapshotdate.strftime(TIME_FORMAT)
        
    if query_type_code.value == QUERY_MARKET.MASTERDATA_METERINGPOINT.value:
        if (start is not None and end is not None):
            payload['Period'] = {"Start": f"{start.strftime(TIME_FORMAT)}", "End": f"{end.strftime(TIME_FORMAT)}"}
        elif snapshotdate is not None:
            payload['SnapShotOccurrence'] = snapshotdate.strftime(TIME_FORMAT)
        else:
            raise ValueError('Query type "MDMP" must have either SnapShotOccurence or Period. Neither were (completely) given.')

    # put everything together
    factory = client.type_factory('ns4')
    eh_request = factory.RequestDataFromElhub(
        Header={
            'Identification': uuid.uuid4(),
            'DocumentType': {
                '_value_1': DOCUMENT_TYPE_UN_CEFACT.QUERY.value,
                'listAgencyIdentifier': LIST_AGENCY_IDENTIFIER.UN_CEFACT.value,
            },
            'Creation': f'{datetime.utcnow().strftime(TIME_FORMAT)}',
            'PhysicalSenderEnergyParty': {
                'Identification': {
                    '_value_1': sender_gsn,
                    'schemeAgencyIdentifier': SCHEME_AGENCY_IDENTIFIER.GS1.value,
                }
            },
            'JuridicalSenderEnergyParty': {
                'Identification': {
                    '_value_1': sender_gsn,
                    'schemeAgencyIdentifier': SCHEME_AGENCY_IDENTIFIER.GS1.value,
                }
            },
            'JuridicalRecipientEnergyParty': {
                'Identification': {'_value_1': ELHUB_GSN, 'schemeAgencyIdentifier': SCHEME_AGENCY_IDENTIFIER.GS1.value}
            },
        },
        ProcessEnergyContext={  # https://dok.elhub.no/ediel2/general#General-Process
            'EnergyBusinessProcess': {
                '_value_1': BSR_IDS.MASTER_DATA.value,
                'listAgencyIdentifier': LIST_AGENCY_IDENTIFIER.ELHUB.value,
            },
            'EnergyBusinessProcessRole': {
                '_value_1': process_role.value,
                'listAgencyIdentifier': LIST_AGENCY_IDENTIFIER.UN_CEFACT.value,
            },
            'EnergyIndustryClassification': "23",
        },
        PayloadMPEvent=payload
    )

    return eh_request


def request_masterdata_customer(
    client: zeep.Client,
    history: HistoryPlugin,
    meter_identificator: str,
    sender_gsn: str,
    snapshotdate: datetime,
    # start: datetime,
    # end: datetime,
    process_role: ROLES = ROLES.THIRD_PARTY,
) -> bool:
    """
    Query WSDL
    Args:
        history:
        client:
        meter_identificator:
        sender_gsn:
        snapshotdate:
        start:
        end:
        process_role:

    Returns:

    """

    eh_request = _create_eh_request(
        query_type_code=QUERY_MARKET.MASTERDATA_CUSTOMER,
        client=client,
        meter_identificator=meter_identificator,
        sender_gsn=sender_gsn,
        snapshotdate=snapshotdate,
        process_role=process_role
    )

    try:
        response = client.service.RequestDataFromElhub(eh_request)
        if history.last_received:
            return True
        logger.error(f"Unknown error: {response}")
    except Exception as ex:
        logger.exception(ex)
    return False


def request_masterdata_metering_point(
    client: zeep.Client,
    history: HistoryPlugin,
    meter_identificator: str,
    sender_gsn: str,
    snapshotdate: datetime = None,
    start: datetime = None,
    end: datetime = None,
    process_role: ROLES = ROLES.THIRD_PARTY,
) -> bool:
    """
    Query WSDL
    Args:
        history:
        client:
        meter_identificator:
        sender_gsn:
        snapshotdate:
        start:
        end:
        process_role:

    Returns:

    """

    eh_request = _create_eh_request(
        query_type_code=QUERY_MARKET.MASTERDATA_METERINGPOINT,
        client=client,
        meter_identificator=meter_identificator,
        sender_gsn=sender_gsn,
        snapshotdate=snapshotdate,
        start=start,
        end=end,
        process_role=process_role
    )

    try:
        response = client.service.RequestDataFromElhub(eh_request)
        if history.last_received:
            return True
        logger.error(f"Unknown error: {response}")
    except Exception as ex:
        logger.exception(ex)
    return False