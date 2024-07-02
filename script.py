import io
import json
import fitz
from openai import OpenAI

def get_pdf_bytes(filepath: str) -> bytes:
    with open(filepath, "rb") as f:
        pdf_bytes = f.read()
        return pdf_bytes

def extract_pdf_text(pdf_bytes: bytes) -> str:
    pdf_text = ''
    pdf_document = fitz.open(PDF_EXT, pdf_bytes)
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        text_page = page.get_textpage()
        pdf_text += text_page.extractText()

    return pdf_text

def extract_pdf_fields(pdf_bytes: bytes) -> list[dict]:
    form_fields = []
    pdf_document = fitz.open(PDF_EXT, pdf_bytes)

    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        widget_list = page.widgets()
        if widget_list:
            for widget in widget_list:
                form_fields.append({
                    'name': widget.field_name,
                    'label': widget.field_label,
                    'type': widget.field_type_string,
                    'max_length': widget.text_maxlen
                })

    return form_fields

def read_pdf(filepath: str) -> tuple[str, list[dict]]:
    pdf_bytes = get_pdf_bytes(filepath)
    text = extract_pdf_text(pdf_bytes)
    fields = extract_pdf_fields(pdf_bytes)
    
    return text, fields

def read_txt(filepath: str) -> str:
    with open(filepath, 'r', encoding='utf-8') as file:
        contents = file.read()
        return contents

def fill_fields_prompt(pdf_text: str, fields: list[dict], source_info: str) -> str:
    return f"""
        You are an automated PDF forms filler.
        Your job is to fill the following form fields using the provided materials.
        Field keys will tell you which values they expect:
        {json.dumps(fields)}

        Materials:
        - Text extracted from the PDF form, delimited by <>:
        <{pdf_text}>

        - Source info attached by user, delimited by ##:
        #{source_info}#
        
        Output a JSON object with key-value pairs where:
        - key is the 'name' of the field,
        - value is the field value you assigned to it.
    """

def call_openai(prompt: str, gpt_model: str = 'gpt-4o'):
    response =  openai_client.chat.completions.create(
        model=gpt_model,
        messages=[{'role': 'system', 'content': prompt}],
        response_format={"type": "json_object"},
        timeout=TIMEOUT,
        temperature=0
    )
    
    response_data = response.choices[0].message.content.strip()
    return json.loads(response_data)

def fill_fields_with_gpt(pdf_text: str, fields: list[dict], source_info: str) -> list[dict]:
    prompt = fill_fields_prompt(pdf_text, fields, source_info)
    filled_fields_dict = call_openai(prompt)
    return filled_fields_dict

def fill_pdf_fields(pdf_bytes: bytes, field_values: dict) -> io.BytesIO:
    pdf_document = fitz.open(PDF_EXT, pdf_bytes)
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        widget_list = page.widgets()

        if widget_list:
            for widget in widget_list:
                field_name = widget.field_name
                if field_name in field_values:
                    widget.field_value = field_values[field_name]
                    widget.update()
                    
    output_stream = io.BytesIO()
    pdf_document.save(output_stream)
    output_stream.seek(0)

    return output_stream

def fill_pdf(fields_dict: dict, input_pdf: str, output_pdf: str):
    pdf_bytes = get_pdf_bytes(input_pdf)
    output_pdf_stream = fill_pdf_fields(pdf_bytes, fields_dict)
    
    with open(output_pdf, "wb") as f:
        f.write(output_pdf_stream.getvalue())

def fill_pdf_with_ai(input_pdf: str, output_pdf: str, source_file: str):
    pdf_text, pdf_fields = read_pdf(input_pdf)
    source_info = read_txt(source_file)
    filled_fields_dict = fill_fields_with_gpt(pdf_text, pdf_fields, source_info)
    
    fill_pdf(filled_fields_dict, input_pdf, output_pdf)

if __name__ == "__main__":
    PDF_EXT = 'pdf'
    TIMEOUT = 500

    openai_client = OpenAI(
        api_key='<your_openai_api_key>'
    )
    
    folder = 'examples'
    form = 'w9'
    
    input_pdf = f'{folder}/{form}.pdf'
    output_pdf = f'{folder}/{form}-filled.pdf'
    source_file = f'{folder}/{form}-info.txt'
    
    fill_pdf_with_ai(input_pdf, output_pdf, source_file)