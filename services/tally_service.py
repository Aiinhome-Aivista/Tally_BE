import requests
import xml.etree.ElementTree as ET
from xml.dom import minidom
import logging
import json
import re

logger = logging.getLogger(__name__)

class TallyService:
    def __init__(self, host: str = "localhost", port: int = 9000):
        self.url = f"http://{host}:{port}"

    def _send_request(self, xml_payload: str):
        headers = {'Content-Type': 'text/xml'}
        try:
            # Increased timeout to 60 seconds because Tally reports can take time to generate
            # Explicitly disable proxies to prevent HTTP_PROXY environment variables from hijacking the request
            response = requests.post(self.url, data=xml_payload, headers=headers, timeout=60, proxies={"http": None, "https": None})
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to Tally: {e}")
            raise Exception(f"Failed to connect to Tally at {self.url}. Is Tally running? Error: {e}")

    def get_ledgers(self):
        """Fetch all ledgers from Tally."""
        xml_payload = """<ENVELOPE>
            <HEADER>
                <TALLYREQUEST>Export Data</TALLYREQUEST>
            </HEADER>
            <BODY>
                <EXPORTDATA>
                    <REQUESTDESC>
                        <REPORTNAME>List of Accounts</REPORTNAME>
                        <STATICVARIABLES>
                            <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                            <ACCOUNTTYPE>Ledgers</ACCOUNTTYPE>
                        </STATICVARIABLES>
                    </REQUESTDESC>
                </EXPORTDATA>
            </BODY>
        </ENVELOPE>"""
        return self._send_request(xml_payload)


    def parse_xml_to_dict(self, xml_string: str):
        """A generic helper to parse Tally XML response into a list of dictionaries."""
        try:
            # Tally XML often contains invalid characters or entities. Let's sanitize it.
            # Remove invalid raw control characters
            clean_xml = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x84\x86-\x9f]', '', xml_string)
            # Remove invalid hex entities (e.g., &#x1C;)
            clean_xml = re.sub(r'&#x([0-8BCEF]|1[0-9A-F]);', '', clean_xml, flags=re.IGNORECASE)
            # Remove invalid decimal entities (e.g., &#28;)
            clean_xml = re.sub(r'&#([0-8]|1[12]|1[4-9]|2[0-9]|3[01]);', '', clean_xml)
            
            root = ET.fromstring(clean_xml)
            data = []
            entity_name = None
            
            # Find the COLLECTION node or fallback to ENVELOPE's direct children
            target_node = None
            for elem in root.iter('COLLECTION'):
                target_node = elem
                break
                
            if target_node is None and root.tag == 'ENVELOPE':
                # Check if ENVELOPE has data children (e.g., custom TDL responses)
                for child in root:
                    if child.tag not in ('HEADER', 'BODY'):
                        target_node = root
                        break
                        
            if target_node is None:
                logger.warning("No COLLECTION node or data records found in XML.")
                return data, entity_name
                
            # Determine the entity type from the first data child of target_node
            for child in target_node:
                if child.tag not in ('HEADER', 'BODY'):
                    entity_name = child.tag
                    break
                    
            if not entity_name:
                return data, entity_name
                
            def element_to_dict(elem):
                """Recursively converts an XML element to a dict."""
                result = {}
                for child in elem:
                    if len(child) == 0:
                        # Leaf node
                        result[child.tag] = child.text if child.text else ""
                    else:
                        # Nested node, could be a list or an object
                        child_dict = element_to_dict(child)
                        if child.tag in result:
                            # If it already exists, convert to list
                            if not isinstance(result[child.tag], list):
                                result[child.tag] = [result[child.tag]]
                            result[child.tag].append(child_dict)
                        else:
                            result[child.tag] = child_dict
                return result

            for item in target_node.iter(entity_name):
                item_data = {}
                
                # Extract attributes of the item itself (like ALTERID)
                if hasattr(item, 'attrib') and item.attrib:
                    for k, v in item.attrib.items():
                        item_data[f"ATTR_{k}"] = v

                for child in item:
                    if len(child) == 0:
                        item_data[child.tag] = child.text if child.text else ""
                    else:
                        # Flatten complex nested structures as JSON string
                        nested_data = element_to_dict(child)
                        item_data[child.tag] = json.dumps(nested_data)
                        
                if item_data:
                    data.append(item_data)
                    
            return data, entity_name
            
        except ET.ParseError as e:
            logger.error(f"XML Parsing Error: {e}")
            return [], None
