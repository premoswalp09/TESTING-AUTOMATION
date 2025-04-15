"""PDF report generation for ETL test results."""
from datetime import datetime
import io
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch

from utils.db_utils import get_object_details

def create_pdf_report(result, metadata):
    """
    Create a PDF report from test results data.
    
    Args:
        result (TestResult): Test result object
        metadata (RunMetadata): Run metadata object
        
    Returns:
        bytes: PDF file content as bytes
    """
    # Create an in-memory buffer for the PDF
    buffer = io.BytesIO()
    
    # Create the PDF document using ReportLab
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    
    # Create custom styles
    title_style = ParagraphStyle(
        'Title',
        parent=styles['Heading1'],
        fontSize=16,
        alignment=1,  # Center alignment
        spaceAfter=0.1*inch
    )
    
    subtitle_style = ParagraphStyle(
        'Subtitle',
        parent=styles['Heading3'],
        fontSize=12,
        alignment=1,  # Center alignment
        spaceAfter=0.3*inch
    )
    
    heading_style = ParagraphStyle(
        'Heading',
        parent=styles['Heading2'],
        fontSize=14,
        spaceAfter=0.2*inch
    )
    
    normal_style = styles['Normal']
    code_style = ParagraphStyle(
        'Code',
        parent=styles['Code'],
        fontName='Courier',
        fontSize=9,
        leading=12
    )
    
    # Create watermark style for "Generated" timestamp
    watermark_style = ParagraphStyle(
        'Watermark',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=8,
        textColor=colors.Color(0, 0, 0, alpha=0.4),  # Translucent black
        alignment=2  # Right alignment
    )
    
    # Current timestamp for the "Generated" text
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Create document elements
    elements = []
    
    # Add title - Just "ETL Test Results Report" without the test ID
    title_text = "ETL Test Results Report"
    title = Paragraph(title_text, title_style)
    elements.append(title)
    
    # Add Test ID as subtitle
    test_id_text = f"{result.ts_id}"
    subtitle = Paragraph(test_id_text, subtitle_style)
    elements.append(subtitle)
    
    # Add the "Generated" timestamp with translucent text
    generated_text = f"Generated: {current_time}"
    generated = Paragraph(generated_text, watermark_style)
    elements.append(generated)
    
    elements.append(Spacer(1, 0.25*inch))
    
    # Add test metadata section
    elements.append(Paragraph("Test Metadata", heading_style))
    
    # Use Paragraph for Test ID to handle wrapping of long IDs
    test_id_para = Paragraph(result.ts_id, normal_style)
    
    metadata_table = [
        ["Test ID", test_id_para],
        ["Validation Type", result.validation_type],
        ["Run ID", metadata.run_id],
        ["Execution Date", str(metadata.execution_date)],
        ["Execution Time (sec)", str(result.execution_time)],
        ["User ID", metadata.user_id]
    ]
    
    metadata_table = Table(metadata_table, colWidths=[2*inch, 4*inch])
    metadata_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.black),
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('VALIGN', (0, 0), (-1, -1), 'TOP')  # Align all cells to top for consistency
    ]))
    elements.append(metadata_table)
    elements.append(Spacer(1, 0.25*inch))
    
    # Add test details section
    elements.append(Paragraph("Test Details", heading_style))
    
    # Get DB, schema and table from TS_ID
    DB, SCH, TAB, OP = get_object_details(result.ts_id)
    
    details = [
        ["Application Name", DB],
        ["EDWH", DB],
        ["EADI", f"{DB}_IDMC"],
        ["Schema Name", SCH],  # Added schema name
        ["Table Name", TAB]    # Added table name
    ]
    
    # Add count information if available
    if result.source_count not in ("NULL", None):
        details.append(["Source Count", str(result.source_count)])
    if result.target_count not in ("NULL", None):
        details.append(["Target Count", str(result.target_count)])
    
    details_table = Table(details, colWidths=[2*inch, 4*inch])
    details_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.black),
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('GRID', (0, 0), (-1, -1), 1, colors.black)
    ]))
    elements.append(details_table)
    elements.append(Spacer(1, 0.25*inch))
    
    # Add executed query with special formatting
    elements.append(Paragraph("Executed Query", heading_style))
    
    # Format the query based on validation type
    if result.validation_type.lower() == "count check" and "|" in result.executed_query:
        query_parts = result.executed_query.split("|")
        formatted_query1 = "1." + query_parts[0].strip()
        formatted_query2 = "2." + query_parts[1].strip()
        query_text1 = Paragraph(formatted_query1, code_style)
        query_text2 = Paragraph(formatted_query2, code_style)

        elements.append(query_text1)    
        elements.append(query_text2)
        elements.append(Spacer(1, 0.25*inch))
    else:
        query_text = Paragraph(result.executed_query, code_style)    
        elements.append(query_text)
        elements.append(Spacer(1, 0.25*inch))
    
    # Add test results section
    elements.append(Paragraph("Test Results", heading_style))
    
    # Define status color
    dark_yellow = colors.Color(0.8, 0.6, 0)  # Custom dark yellow color
    
    status_color = (
        colors.green if result.status == 'Pass' 
        else dark_yellow if result.status == 'Error' 
        else colors.red
    )
    
    results = [
        ["Status", result.status],
        ["Expected Result", result.expected_result],
        ["Actual Result", result.actual_result]
    ]
    
    # Create a list for the error description and file path
    additional_rows = []
    
    # Add error description and minus query file path if relevant
    if result.error_description != "N/A":
        additional_rows.append(["Error Description", result.error_description])
    if result.minus_query_file_path != "N/A":
        additional_rows.append(["Minus Query File Path", result.minus_query_file_path])
    
    # First create the main results table
    results_table = Table(results, colWidths=[2*inch, 4*inch])
    results_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (0, -1), colors.lightgrey),
        ('TEXTCOLOR', (0, 0), (0, -1), colors.black),
        ('ALIGN', (0, 0), (0, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
        ('GRID', (0, 0), (-1, -1), 1, colors.black),
        ('BACKGROUND', (1, 0), (1, 0), status_color),
        ('TEXTCOLOR', (1, 0), (1, 0), colors.white)
    ]))
    elements.append(results_table)
    elements.append(Spacer(1, 0.1*inch))
    
    # Create separate tables for error description and file path if they exist
    # This allows for better wrapping of long text
    for label, value in additional_rows:
        # Create a paragraph style for potential long text
        long_text_style = ParagraphStyle(
            'LongText',
            parent=styles['Normal'],
            fontSize=10,
            wordWrap='CJK',
            leading=12
        )
        
        # Create a paragraph object for the value to enable wrapping
        value_paragraph = Paragraph(value, long_text_style)
        
        # Add a table with just this row
        extra_table = Table([[label, value_paragraph]], colWidths=[2*inch, 4*inch])
        extra_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, 0), colors.lightgrey),
            ('TEXTCOLOR', (0, 0), (0, 0), colors.black),
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('VALIGN', (0, 0), (1, 0), 'TOP'),
            ('FONTNAME', (0, 0), (0, 0), 'Helvetica'),
            ('FONTSIZE', (0, 0), (1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (1, 0), 6),
            ('GRID', (0, 0), (1, 0), 1, colors.black)
        ]))
        elements.append(extra_table)
    
    # Build the PDF
    doc.build(elements)
    
    # Get the PDF value
    pdf_value = buffer.getvalue()
    buffer.close()
    
    return pdf_value