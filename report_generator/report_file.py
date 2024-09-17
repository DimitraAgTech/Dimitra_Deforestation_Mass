import os
import uuid

from fpdf import FPDF
from PIL import Image

app_root = os.getcwd()

REPORT_LOCATION = "temp"


class ReportPdf(FPDF):
    def header(self):
        self.image(
            os.path.join(app_root, "assets", "template.jpeg"),
            x=0,
            y=0,
            w=217,
            h=355,
            type="",
            link="",
        )
        self.set_text_color(24, 87, 15)
        self.set_draw_color(24, 87, 15)

    def footer(self):
        pass


class ReportGenerator:
    area_description = "Area of {:.2f} acre."

    def __init__(self, results):
        self.results = results

    def get_aspect_ratio(self, image_path):
        with Image.open(image_path) as img:
            width, height = img.size
            aspect_ratio = width / height
            return aspect_ratio

    def align_deforestation_image(self, deforestation_image, max_size):
        aspect_ratio = self.get_aspect_ratio(deforestation_image)

        w, h = max_size, max_size
        x_pad, y_pad = 0, 0
        if aspect_ratio > 1:
            w = max_size
            h = w / aspect_ratio
            y_pad = (max_size - h) / 2
        if aspect_ratio < 1:
            h = max_size
            w = h * aspect_ratio
            x_pad = (max_size - w) / 2

        return w, h, x_pad, y_pad

    def add_polygon_page(self, pdf, result):
        spacer = 10
        margin_x = 12
        info_text_start_y = 42 + spacer + 90 + 15
        info_text_spacing = 14

        polygon_name = result["polygon_name"]
        deforestation_image = result["deforestation_image"]
        google_polygon_image = result["google_polygon_image"]
        total_area = f"{result['totalArea']:.2f}"

        very_high_prob = f"{result['veryHighProb']:.2f}"
        very_high_prob_percent = f"{result['veryHighProbPercent']:.2f}"
        high_prob = f"{result['highProb']:.2f}"
        high_prob_percent = f"{result['highProbPercent']:.2f}"
        medium_prob = f"{result['mediumProb']:.2f}"
        medium_prob_percent = f"{result['mediumProbPercent']:.2f}"
        low_prob = f"{result['lowProb']:.2f}"
        low_prob_percent = f"{result['lowProbPercent']:.2f}"
        very_low_prob = f"{result['veryLowProb']:.2f}"
        very_low_prob_percent = f"{result['veryLowProbPercent']:.2f}"
        zero_prob = f"{result['zeroProb']:.2f}"
        zero_prob_percent = f"{result['zeroProbPercent']:.2f}"
        overallProb = result["overallProb"]
        protectedAreasAlerts = ", ".join(result["protectedAreasAlerts"])
        indigenousLand = ", ".join(result["indigenousLand"])

        pdf.add_page()
        pdf.set_text_color(72, 72, 72)
        pdf.set_font("Times", "B", 10 + 6)
        pdf.set_xy(margin_x, 30 + spacer)
        pdf.cell(pdf.w, 10, f"FARM: {polygon_name}")

        # add google satellite image
        pdf.image(google_polygon_image, x=12, y=42 + spacer, w=90, h=90)
        # add image for deforestation
        pdf.set_draw_color(0, 128, 128)
        pdf.set_line_width(0.3)
        pdf.rect(x=margin_x + 90 + 12, y=42 + spacer, w=90, h=90)

        w, h, x_pad, y_pad = self.align_deforestation_image(deforestation_image, 90 - 4)
        pdf.image(
            deforestation_image,
            x=margin_x + 90 + 12 + 2 + x_pad,
            y=42 + spacer + 2 + y_pad,
            w=w,
            h=h,
        )

        # create 6 boxes for colors
        pdf.set_draw_color(0, 0, 0)

        pdf.set_fill_color(139, 0, 0)  # very high probability color
        pdf.rect(
            margin_x,
            info_text_start_y + info_text_spacing,
            6,
            6,
            "DF",
            round_corners=True,
            corner_radius=1,
        )

        pdf.set_fill_color(240, 32, 32)  # high probability color
        pdf.rect(
            margin_x,
            info_text_start_y + info_text_spacing * 2,
            6,
            6,
            "DF",
            round_corners=True,
            corner_radius=1,
        )

        pdf.set_fill_color(255, 165, 32)  # medium probability color
        pdf.rect(
            margin_x,
            info_text_start_y + info_text_spacing * 3,
            6,
            6,
            "DF",
            round_corners=True,
            corner_radius=1,
        )

        pdf.set_fill_color(96, 219, 219)  # low probability color
        pdf.rect(
            margin_x,
            info_text_start_y + info_text_spacing * 4,
            6,
            6,
            "DF",
            round_corners=True,
            corner_radius=1,
        )

        pdf.set_fill_color(32, 128, 128)  # very low probability color
        pdf.rect(
            margin_x,
            info_text_start_y + info_text_spacing * 5,
            6,
            6,
            "DF",
            round_corners=True,
            corner_radius=1,
        )

        pdf.set_fill_color(255, 255, 255)  # zero probability color
        pdf.rect(
            margin_x,
            info_text_start_y + info_text_spacing * 6,
            6,
            6,
            "DF",
            round_corners=True,
            corner_radius=1,
        )

        # Text for headings
        pdf.set_text_color(50, 50, 50)
        pdf.set_font("Helvetica", "B", 10 + 6)
        pdf.set_xy(margin_x, info_text_start_y)
        pdf.cell(pdf.w, 6, "Total Area")

        # Text for subheadings
        pdf.set_font("Helvetica", "B", 10 + 2)
        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing - 1)
        pdf.cell(pdf.w, 6, "Very High Deforestation Probability")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing * 2 - 1)
        pdf.cell(pdf.w, 6, "High Deforestation Probability")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing * 3 - 1)
        pdf.cell(pdf.w, 6, "Medium Deforestation Probability")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing * 4 - 1)
        pdf.cell(pdf.w, 6, "Low Deforestation Probability")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing * 5 - 1)
        pdf.cell(pdf.w, 6, "Very Low Deforestation Probability")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing * 6 - 1)
        pdf.cell(pdf.w, 6, "Zero/Negligible Deforestation Probability")

        pdf.set_xy(margin_x, info_text_start_y + info_text_spacing * 7 - 1)
        pdf.cell(pdf.w, 6, "Overall Deforestation Probability")

        # Text for data
        pdf.set_font("Helvetica", "", 10)
        pdf.set_text_color(64, 64, 64)
        pdf.set_xy(margin_x, info_text_start_y + 6)
        pdf.cell(pdf.w, 6, f"{total_area} HA")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing + 4)
        pdf.cell(pdf.w, 6, f"{very_high_prob} HA ({very_high_prob_percent}%)")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing * 2 + 4)
        pdf.cell(pdf.w, 6, f"{high_prob} HA ({high_prob_percent}%)")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing * 3 + 4)
        pdf.cell(pdf.w, 6, f"{medium_prob} HA ({medium_prob_percent}%)")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing * 4 + 4)
        pdf.cell(pdf.w, 6, f"{low_prob} HA ({low_prob_percent}%)")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing * 5 + 4)
        pdf.cell(pdf.w, 6, f"{very_low_prob} HA ({very_low_prob_percent}%)")

        pdf.set_xy(margin_x + 8, info_text_start_y + info_text_spacing * 6 + 4)
        pdf.cell(pdf.w, 6, f"{zero_prob} HA ({zero_prob_percent}%)")

        pdf.set_xy(margin_x, info_text_start_y + info_text_spacing * 7 + 4)
        pdf.cell(pdf.w, 6, overallProb)

        # Protected areas section
        pdf.set_text_color(50, 50, 50)
        pdf.set_font("Helvetica", "B", 10 + 2)
        pdf.set_xy(margin_x, info_text_start_y + info_text_spacing * 8.5)
        pdf.cell(
            pdf.w,
            6,
            "Protected Areas and/or Indigenous Lands Intercepted by the Geofence(s)",
        )

        pdf.set_text_color(64, 64, 64)
        pdf.set_font("Helvetica", "", 11)
        pdf.set_xy(margin_x, info_text_start_y + info_text_spacing * 8.5 + 8)
        pdf.write(6, "Type(s) of protected area(s) intercepted by the geofence(s): ")

        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "B", 11)
        pdf.write(6, protectedAreasAlerts)

        pdf.set_xy(margin_x, info_text_start_y + info_text_spacing * 8.5 + 14)
        pdf.set_text_color(64, 64, 64)
        pdf.set_font("Helvetica", "", 11)
        pdf.write(
            6,
            "Does the geofence(s) intercept indigenous land(s) recognized by the government?: ",
        )

        pdf.set_text_color(0, 0, 0)
        pdf.set_font("Helvetica", "B", 11)
        pdf.write(6, indigenousLand)

        return pdf

    def generate(self):
        pdf = ReportPdf("P", "mm", "legal")
        for result in self.results:
            pdf = self.add_polygon_page(pdf, result)

        filename = f"{uuid.uuid4()}.pdf"

        pdf_path = os.path.join(REPORT_LOCATION, filename)
        pdf.output(pdf_path)

        return pdf_path, filename
