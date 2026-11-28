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
            # Increased timeout to 600 seconds because Tally reports like VCHLED can take a long time to generate
            # Explicitly disable proxies to prevent HTTP_PROXY environment variables from hijacking the request
            response = requests.post(self.url, data=xml_payload, headers=headers, timeout=600, proxies={"http": None, "https": None})
            response.raise_for_status()
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to Tally: {e}")
            raise Exception(f"Failed to connect to Tally at {self.url}. Is Tally running? Error: {e}")

    def _send_request_stream(self, xml_payload: str):
        """Sends a request to Tally and returns a streaming response to prevent memory issues."""
        headers = {'Content-Type': 'text/xml'}
        try:
            response = requests.post(self.url, data=xml_payload, headers=headers, timeout=600, proxies={"http": None, "https": None}, stream=True)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"Error connecting to Tally (Stream): {e}")
            raise Exception(f"Failed to connect to Tally at {self.url}. Error: {e}")

    def parse_xml_iteratively(self, response):
        """
        Parses Tally XML response as a stream using iterparse.
        Cleans invalid Tally characters/tags per chunk safely,
        handling chunk boundaries so <> or </> are never split.
        """
        import xml.etree.ElementTree as ET
        import re
        self.last_blank_tags_removed = 0
        self._debug_xml_lines = []
        self.last_xml_stats = {
            "total_xml_records": 0,
            "fetched": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "empty_named_fields": 0,
            "malformed_pairs_removed": 0,
            "malformed_tokens_removed": 0,
            "parent_record_name": "",
            "sample_records": [],
            "sample_invalid_records": [],
            "malformed_examples": [],
            "field_audit": {},
        }

        def clean_xml_stream(resp):
            def valid_entity(match):
                text = match.group(0)
                try:
                    val = int(text[3:-1], 16) if text.lower().startswith('&#x') else int(text[2:-1])
                    if (val == 0x9 or val == 0xA or val == 0xD or
                            (0x20 <= val <= 0xD7FF) or
                            (0xE000 <= val <= 0xFFFD) or
                            (0x10000 <= val <= 0x10FFFF)):
                        return text
                    return ''
                except Exception:
                    return ''

            leftover = ''
            for chunk in resp.iter_content(chunk_size=1024 * 64, decode_unicode=True):
                if not chunk:
                    continue

                to_process = leftover + chunk

                # Hold back up to 10 chars at the end in case a tag/entity is split
                # across this chunk and the next (e.g. chunk ends with '<' or '&')
                safe_end = len(to_process)
                for tail in range(1, 11):
                    c = to_process[safe_end - tail]
                    if c in ('<', '&'):
                        safe_end = safe_end - tail
                        break

                leftover = to_process[safe_end:]
                to_process = to_process[:safe_end]

                # Keep trailing '<>' in leftover so '<></>' split across chunk boundaries is counted as a pair
                trailing_open = re.search(r'<\s*>\s*$', to_process)
                if trailing_open:
                    leftover = to_process[trailing_open.start():] + leftover
                    to_process = to_process[:trailing_open.start()]

                if not to_process:
                    continue

                # Remove raw control characters (keep tab/CR/LF)
                cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x84\x86-\x9f]', '', to_process)
                # Remove invalid XML numeric character references
                cleaned = re.sub(r'&#[xX]?[0-9a-fA-F]+;', valid_entity, cleaned)
                # 1. Remove malformed empty tag pairs <></> (1 pair = 2 individual tokens)
                if len(self.last_xml_stats["malformed_examples"]) < 2 and re.search(r'<\s*>\s*<\s*/\s*>', cleaned):
                    self.last_xml_stats["malformed_examples"].append("<></>")
                cleaned, n_pairs = re.subn(r'<\s*>\s*<\s*/\s*>', '', cleaned)
                self.last_xml_stats["malformed_pairs_removed"] += n_pairs
                self.last_xml_stats["malformed_tokens_removed"] += n_pairs * 2
                # 2. Remove any remaining individual malformed <> or </> tokens
                if len(self.last_xml_stats["malformed_examples"]) < 2:
                    m_tok = re.search(r'<\s*/?\s*>', cleaned)
                    if m_tok:
                        self.last_xml_stats["malformed_examples"].append(m_tok.group(0))
                cleaned, n_singles = re.subn(r'<\s*/?\s*>', '', cleaned)
                self.last_xml_stats["malformed_tokens_removed"] += n_singles
                self.last_blank_tags_removed = self.last_xml_stats["malformed_tokens_removed"]
                if len(self._debug_xml_lines) < 200:
                    self._debug_xml_lines.extend(cleaned.splitlines())

                yield cleaned.encode('utf-8')

            # Flush any leftover
            if leftover:
                cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x84\x86-\x9f]', '', leftover)
                cleaned = re.sub(r'&#[xX]?[0-9a-fA-F]+;', valid_entity, cleaned)
                cleaned, n_pairs = re.subn(r'<\s*>\s*<\s*/\s*>', '', cleaned)
                self.last_xml_stats["malformed_pairs_removed"] += n_pairs
                self.last_xml_stats["malformed_tokens_removed"] += n_pairs * 2
                cleaned, n_singles = re.subn(r'<\s*/?\s*>', '', cleaned)
                self.last_xml_stats["malformed_tokens_removed"] += n_singles
                self.last_blank_tags_removed = self.last_xml_stats["malformed_tokens_removed"]
                if cleaned.strip():
                    yield cleaned.encode('utf-8')

        def element_to_dict(elem):
            result = {}
            for child in elem:
                if len(child) == 0:
                    result[child.tag] = child.text if child.text else ""
                else:
                    child_dict = element_to_dict(child)
                    if child.tag in result:
                        if not isinstance(result[child.tag], list):
                            result[child.tag] = [result[child.tag]]
                        result[child.tag].append(child_dict)
                    else:
                        result[child.tag] = child_dict
            return result

        class FileLikeIter:
            def __init__(self, iterator):
                self.iterator = iterator
                self.buffer = b''

            def read(self, size=-1):
                if size == -1:
                    data = [self.buffer] + list(self.iterator)
                    self.buffer = b''
                    return b''.join(data)
                while len(self.buffer) < size:
                    try:
                        self.buffer += next(self.iterator)
                    except StopIteration:
                        break
                result = self.buffer[:size]
                self.buffer = self.buffer[size:]
                return result

        stream = FileLikeIter(clean_xml_stream(response))

        depth = 0
        current_record = {}
        current_base_entity = None
        skip_tags = {'HEADER', 'DESC', 'STATICVARIABLES', 'IMPORTDATA', 'EXPORTDATA', 'BODY'}

        def _record_audit(rec, entity_name):
            stats = self.last_xml_stats
            stats["total_xml_records"] += 1
            stats["fetched"] += 1
            stats["valid_records"] += 1
            if not stats["parent_record_name"] and entity_name:
                stats["parent_record_name"] = entity_name
            # Find an identifier for this record dynamically
            rec_id = ""
            for k, v in rec.items():
                if v and any(tok in k.upper() for tok in ("MASTERID", "ALTERID", "VOUCHERNUMBER", "GUID", "ID")):
                    rec_id = str(v)
                    break
            if len(stats["sample_records"]) < 2:
                # Store compact preview (first 8 fields)
                preview = {k: rec[k] for k in list(rec.keys())[:8]}
                stats["sample_records"].append({"id": rec_id or "N/A", "entity": entity_name, "fields": preview})
            fa = stats["field_audit"]
            for k, v in rec.items():
                is_empty = (v == "" or v is None)
                if is_empty:
                    stats["empty_named_fields"] += 1
                entry = fa.get(k)
                if entry is None:
                    entry = {
                        "empty": 0,
                        "filled": 0,
                        "total": 0,
                        "empty_example": "",
                        "filled_example": "",
                        "sample_value": "",
                        "record_id": "",
                    }
                    fa[k] = entry
                entry["total"] += 1
                if is_empty:
                    entry["empty"] += 1
                    if not entry["empty_example"]:
                        entry["empty_example"] = f"<{k}></{k}>"
                        if not entry["record_id"] and rec_id:
                            entry["record_id"] = rec_id
                else:
                    entry["filled"] += 1
                    if not entry["filled_example"]:
                        val_str = str(v)[:80]
                        entry["filled_example"] = f"<{k}>{val_str}</{k}>"
                        entry["sample_value"] = val_str
                        if rec_id:
                            entry["record_id"] = rec_id

        try:
            context = ET.iterparse(stream, events=("start", "end"))
            for event, elem in context:
                if event == "start":
                    depth += 1
                elif event == "end":
                    depth -= 1
                    if elem.tag == "LINEERROR" and elem.text and elem.text.strip():
                        raise Exception(f"Tally Error: {elem.text.strip()}")

                    if depth == 1 and elem.tag not in skip_tags:
                        base_entity = re.sub(r'\d+$', '', elem.tag)
                        item_data = {}

                        if hasattr(elem, 'attrib') and elem.attrib:
                            for k, v in elem.attrib.items():
                                item_data[f"ATTR_{k}"] = v

                        if len(elem) == 0:
                            item_data[elem.tag] = elem.text if elem.text else ""
                        else:
                            for child in list(elem):
                                if len(child) == 0:
                                    item_data[child.tag] = child.text if child.text else ""
                                else:
                                    import json
                                    item_data[child.tag] = json.dumps(element_to_dict(child))

                        if item_data:
                            should_flush = False
                            if current_base_entity is not None and base_entity != current_base_entity:
                                should_flush = True
                            else:
                                for k in item_data:
                                    if k in current_record:
                                        should_flush = True
                                        break

                            if should_flush and current_record:
                                _record_audit(current_record, current_base_entity)
                                yield current_record, current_base_entity
                                current_record = {}

                            current_record.update(item_data)
                            current_base_entity = base_entity

                        elem.clear()

            if current_record and current_base_entity:
                _record_audit(current_record, current_base_entity)
                yield current_record, current_base_entity

        except ET.ParseError as e:
            err_detail = str(e)
            try:
                line_no = e.position[0] if hasattr(e, "position") and e.position else None
                if line_no and 1 <= line_no <= len(self._debug_xml_lines):
                    bad_line = self._debug_xml_lines[line_no - 1].strip()
                    if bad_line:
                        err_detail = f"{e} | Line {line_no}: {bad_line[:120]}"
            except Exception:
                pass
            logger.error(f"XML ParseError: {err_detail}")
            raise Exception(f"Failed to parse XML from Tally: {err_detail}")



    def test_connection(self):
        """Test Tally connectivity by querying the active open company."""
        xml_payload = """<ENVELOPE Action="">
  <HEADER>
    <VERSION>1</VERSION>
    <TALLYREQUEST>EXPORT</TALLYREQUEST>
    <TYPE>COLLECTION</TYPE>
    <ID>CUSTOMCOMPANYCOL</ID>
  </HEADER>
  <BODY>
    <DESC>
      <STATICVARIABLES />
      <TDL>
        <TDLMESSAGE>
          <COLLECTION ISMODIFY="No" ISFIXED="No" ISINITIALIZE="Yes" ISOPTION="No" ISINTERNAL="No" NAME="CUSTOMCOMPANYCOL">
            <TYPE>COMPANY</TYPE>
            <NATIVEMETHOD>NAME</NATIVEMETHOD>
            <NATIVEMETHOD>GUID</NATIVEMETHOD>
            <NATIVEMETHOD>BOOKSFROM</NATIVEMETHOD>
            <NATIVEMETHOD>STARTINGFROM</NATIVEMETHOD>
            <NATIVEMETHOD>COMPANYNUMBER</NATIVEMETHOD>
            <NATIVEMETHOD>ENDINGAT</NATIVEMETHOD>
            <FILTERS>ActiveCompFilt</FILTERS>
          </COLLECTION>
          <SYSTEM TYPE="Formulae" NAME="ActiveCompFilt">$Name = ##SVCURRENTCOMPANY</SYSTEM>
        </TDLMESSAGE>
      </TDL>
    </DESC>
  </BODY>
</ENVELOPE>"""
        return self._send_request(xml_payload)

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

            # If the chosen entity_name represents a leaf node (len == 0), 
            # it means Tally returned a flat single-record response where fields are direct children of target_node.
            sample_entity = target_node.find(entity_name)
            if sample_entity is not None and len(sample_entity) == 0:
                item_data = {}
                for child in target_node:
                    if child.tag not in ('HEADER', 'BODY'):
                        # If the tag is already in the current item, it means a new row has started
                        if child.tag in item_data:
                            data.append(item_data)
                            item_data = {}
                        item_data[child.tag] = child.text if child.text else ""
                
                # We can name the entity after the base name by removing trailing numbers, e.g., VCHLED1 -> VCHLED
                base_entity = re.sub(r'\d+$', '', entity_name)
                if not base_entity:
                    base_entity = entity_name
                    
                # Append the last row
                if item_data:
                    data.append(item_data)
                return data, base_entity
            else:
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
