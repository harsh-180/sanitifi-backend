import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

import re
import os
import subprocess
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly
import plotly.express as px
import plotly.graph_objects as go
import scipy
import sklearn
import statsmodels
import multiprocessing
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace
from pyspark.sql.utils import AnalysisException
from pptx import Presentation
from pptx.chart.data import CategoryChartData, XyChartData
from pptx.enum.chart import XL_CHART_TYPE
from pptx.util import Inches, Pt
from pptx.dml.color import RGBColor
import openpyxl
import pandas as pd
import uuid
from io import BytesIO
from datetime import datetime  # Added datetime import
from rest_framework.response import Response
from rest_framework.views import APIView
from rest_framework_simplejwt.tokens import RefreshToken
from django.contrib.auth import authenticate
from api.models import User,Projects, SavedScript, SavedPlot
from rest_framework.parsers import MultiPartParser, FormParser
from django.shortcuts import get_object_or_404

from django.conf import settings
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from .models import Projects, User

from .auth import CustomAuthBackend
import shutil

from openpyxl import load_workbook

import pyarrow

from concurrent.futures import ThreadPoolExecutor, as_completed

from django.core.files.storage import default_storage
from .spark_utils import get_spark_session
import threading
import json
from django.http import FileResponse
from django.http import HttpResponse
# Add this at the top of views.py. a global dictionary to hold cleaned data for each session or file:
# cleaned_files_cache = {}
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator
from django.http import StreamingHttpResponse
import subprocess
import traceback
import stat
import csv
import io
   
from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference, PieChart, LineChart
from openpyxl.utils.dataframe import dataframe_to_rows
from rest_framework.parsers import JSONParser

from django.http import HttpResponse
import tempfile
import math

import math

from google.oauth2 import service_account
from googleapiclient.discovery import build

GOOGLE_SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__), 'sanitifi-461410-e0eabc4f9cf6.json')

def get_gsheet_service():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=GOOGLE_SCOPES)
    sheets_service = build('sheets', 'v4', credentials=creds)
    drive_service = build('drive', 'v3', credentials=creds)
    return sheets_service, drive_service

def make_json_safe(obj):
    if isinstance(obj, dict):
        return {k: make_json_safe(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [make_json_safe(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    elif isinstance(obj, (np.floating, np.integer)):
        # Handle numpy float/int types
        if np.isnan(obj) or np.isinf(obj):
            return None
        return obj.item() if hasattr(obj, 'item') else float(obj)
    else:
        return obj
    
@method_decorator(csrf_exempt, name='dispatch')

class MergeFile(APIView):
    def post(self, request):
        # Extract data from request  
        sheet_name = request.data.get('sheet_name')
        project_id = request.data.get('project_id')
        file_name = request.data.get('file_name')
        merge_file = request.FILES.get('merge_file')
        common_columns = request.data.get('common_columns')
        file_type = request.data.get('file_type')
        
        # file_type = file_name.split("\\")[0]
        file_name=file_name.split("\\")[1]

        print('Starting merge process...')
        print(f'Project ID: {project_id}')
        print(f'File Name: {file_name}')
        print(f'Sheet Name: {sheet_name}')
        
        # Clean up file_name if it contains media path
        if file_name and (file_name.startswith("media\\") or file_name.startswith("media/")):
            file_name = file_name.replace("media\\", "").replace("media/", "")

        # Ensure project exists
        try:
            project = Projects.objects.get(id=project_id)
            print(f'User ID: {project.user.id}')
        except Projects.DoesNotExist:
            return Response({'error': 'Project not found'}, status=404)

        try:
            # Correct KPI directory
            base_folder = os.path.join(settings.MEDIA_ROOT, f'user_{project.user.id}/project_{project.id}/{file_type}')
            print(f'Base folder: {base_folder}')
            
            # Determine the correct file path
            if os.path.isdir(os.path.join(base_folder, file_name)):
                # If file_name is a directory and sheet_name is a file
                original_file_path = os.path.join(base_folder, file_name, sheet_name)
            else:
                # If file_name is part of the file path
                original_file_path = os.path.join(base_folder, file_name, sheet_name)
            
            original_file_path = os.path.normpath(original_file_path)
            print(f'Original file path -> {original_file_path}')
            
            # Check if the file exists
            if not os.path.exists(original_file_path):
                return Response({'error': f'File not found at: {original_file_path}'}, status=404)
            
            print("2");

            # Load original and merge files
            original_df = pd.read_csv(original_file_path) if original_file_path.endswith('.csv') else pd.read_excel(original_file_path)
            merge_df = pd.read_excel(merge_file)

            print('Original File Columns:', original_df.columns.tolist())
            print('Merge File Columns:', merge_df.columns.tolist())
            print("3");

            # Standardizing column names (strip + lowercase)
            original_df.columns = original_df.columns.str.strip()
            merge_df.columns = merge_df.columns.str.strip()

            # Ensure common_columns is a proper list
            if isinstance(common_columns, str):
                try:
                    common_columns = eval(common_columns)
                except Exception as e:
                    return Response({'error': f'Invalid common_columns format: {str(e)}'}, status=400)

            if not isinstance(common_columns, list):
                return Response({'error': 'common_columns should be a list'}, status=400)

            common_columns = [col.strip() for col in common_columns]

            print('Common Columns for Merging:', common_columns)

            # Validate all common columns exist in both
            for col in common_columns:
                if col not in original_df.columns:
                    return Response({'error': f"Column '{col}' not found in original file"}, status=400)
                if col not in merge_df.columns:
                    return Response({'error': f"Column '{col}' not found in merge file"}, status=400)

            # Convert common columns to string to avoid merge issues
            for col in common_columns:
                original_df[col] = original_df[col].fillna('').astype(str)
                merge_df[col] = merge_df[col].fillna('').astype(str)
                print(f"Converted column '{col}' to string")

            # ---- MODIFIED MERGE LOGIC START ----

            # Get unique columns from each dataframe (excluding common columns)
            original_only_columns = [col for col in original_df.columns if col not in common_columns]
            merge_only_columns = [col for col in merge_df.columns if col not in common_columns]
            
            # Filter out any duplicates between original_only and merge_only (keeping original file's columns)
            merge_only_columns = [col for col in merge_only_columns if col not in original_only_columns]
            
            # Perform the merge on common columns
            merged_df = pd.merge(
                original_df[common_columns + original_only_columns], 
                merge_df[common_columns + merge_only_columns],
                how='outer', 
                on=common_columns
            )
            # ---- MODIFIED MERGE LOGIC END ----
            
            merged_df.drop_duplicates(inplace=True)
            merged_df = merged_df.replace({np.nan: None})

            # Get file directory for git commit
            file_dir = os.path.dirname(original_file_path)
            relative_file_dir = os.path.relpath(file_dir, start=os.path.join(settings.MEDIA_ROOT, f'user_{project.user.id}/project_{project.id}'))
            file_basename = os.path.basename(original_file_path)

            # Save back to original file (overwrite)
            if original_file_path.endswith('.csv'):
                merged_df.to_csv(original_file_path, index=False)
            else:
                merged_df.to_excel(original_file_path, index=False)

            # Git Commit
            self.commit_to_git(
                os.path.join(settings.MEDIA_ROOT, f'user_{project.user.id}/project_{project.id}'),
                project.user,
                project.id,
                relative_file_dir,
                file_basename
            )

            return Response({
                "message": "Merged and updated the original file successfully",
                "merged_file": {
                    "columns": merged_df.columns.tolist(),
                    "data": merged_df.values.tolist()
                }
            }, status=200)

        except Exception as e:
            print(f'Error: {str(e)}')
            return Response({'error': str(e)}, status=500)

    def commit_to_git(self, project_folder, user, project_id, file_subfolder, sheet_name):
        try:
            if not os.path.exists(os.path.join(project_folder, ".git")):
                subprocess.run(["git", "init"], cwd=project_folder)
                subprocess.run(["git", "config", "user.name", user.name], cwd=project_folder)
                subprocess.run(["git", "config", "user.email", user.email], cwd=project_folder)

            file_path_relative = os.path.join(file_subfolder, sheet_name).replace("\\", "/")
            subprocess.run(["git", "add", file_path_relative], cwd=project_folder)
            commit_message = f"merge - {user.id}/{project_id}/{file_subfolder}/{sheet_name}"
            subprocess.run(["git", "commit", "-m", commit_message], cwd=project_folder)
            print(f"Git commit done for {file_path_relative}")
        except Exception as e:
            print(f"Git commit failed: {str(e)}")


class Save(APIView):
    def post(self, request):
        try:
            # Extract payload data
            sheet_name = request.data.get("sheet_name")  # e.g., "Jan - Dec (2).csv"
            project_id = request.data.get("project_id")
            file_name = request.data.get("file_name")  # e.g., "sample _sg_fronx"
            file_type = request.data.get("file_type")  # Expected: "media" or "kpi"
            data_stage = request.data.get("data_stage")  # Expected: "cleaned" or other stages

            print("\nReceived Save Request with Payload:")
            print(f"  - sheet_name: {sheet_name}")
            print(f"  - project_id: {project_id}")
            print(f"  - file_name: {file_name}")
            print(f"  - file_type: {file_type}")
            print(f"  - data_stage: {data_stage}")

            # Validate project existence
            try:
                project = Projects.objects.get(id=project_id)
                print(f"Project found: user_{project.user.id}/project_{project.id}")
            except Projects.DoesNotExist:
                return Response({"error": "Project not found"}, status=404)

            # Clean up file_name if it contains path information
            file_name = os.path.basename(file_name)
            if file_name and (file_name.startswith("media\\") or file_name.startswith("media/")):
                file_name = file_name.replace("media\\", "").replace("media/", "")

            # Validate file_type
            if file_type not in ["media", "kpi"]:
                return Response({"error": "Invalid file_type. Must be 'media' or 'kpi'"}, status=400)

            # Construct correct file path inside project folder using file_type directly
            file_path = os.path.join(
                settings.MEDIA_ROOT,
                f"user_{project.user.id}/project_{project.id}/{file_type}/{file_name}/{sheet_name}"
            )
            file_path = os.path.normpath(file_path)  # Normalize path
            print(f"Checking file at: {file_path}")

            # Check if the file exists
            if not os.path.exists(file_path):
                print(f"File not found: {file_path}")

                # Debugging - List actual files in the directory
                directory = os.path.dirname(file_path)
                if os.path.exists(directory):
                    print(f"Listing files in {directory}:")
                    print(os.listdir(directory))  # Print all files in the directory
                else:
                    print(f"Directory does not exist: {directory}")

                return Response({"error": "File not found", "expected_path": file_path}, status=404)

            # Extract file extension and validate
            file_extension = os.path.splitext(sheet_name)[-1].lower()
            print(f"Extracted file extension: {file_extension}")

            if file_extension not in [".xlsx", ".csv"]:
                return Response({"error": "Only XLSX and CSV files are supported"}, status=400)

            output = BytesIO()  # In-memory buffer for streaming response

            # Load the file based on its format
            if file_extension == ".xlsx":
                # Load all sheet names
                sheet_names = pd.ExcelFile(file_path, engine="openpyxl").sheet_names
                print(f"Available sheets: {sheet_names}")

                # Ensure the requested sheet exists
                if sheet_name not in sheet_names:
                    return Response({
                        "error": f"Sheet '{sheet_name}' not found in {actual_file_name}.",
                        "available_sheets": sheet_names
                    }, status=400)

                # Read the requested sheet
                df = pd.read_excel(file_path, sheet_name=sheet_name, engine="openpyxl")
            elif file_extension == ".csv":
                df = pd.read_csv(file_path)  # Let pandas infer types

            # Convert numeric-like columns back to numeric format
            for col in df.select_dtypes(include=["object"]).columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors="ignore")
                except Exception:
                    pass

            # Save processed file in memory
            if file_extension == ".xlsx":
                df.to_excel(output, index=False, engine="openpyxl")
            else:
                df.to_csv(output, index=False)

            output.seek(0)  # Move buffer position to the beginning

            # Return file as a streaming response
            response = StreamingHttpResponse(output, content_type="application/octet-stream")
            response['Content-Disposition'] = f'attachment; filename="{sheet_name}"'
            response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response['Pragma'] = 'no-cache'
            response['Expires'] = '0'
            return response

        except Exception as e:
            print(f"Error in Save API: {str(e)}")
            return Response({"error": f"Internal server error: {str(e)}"}, status=500)


class Mapping(APIView):
    def post(self, request):
        sheet_name = request.data.get("sheet_name")  
        project_id = request.data.get("project_id")
        file_name = request.data.get("file_name")  
        column_mappings = request.data.get("column_mappings")  
        mapping_file = request.FILES.get("mapping_file")
        file_type=request.data.get("file_type")
        file_name = os.path.basename(file_name)
        if not project_id or not file_name or not mapping_file or not column_mappings or not sheet_name:
            return Response({"error": "Missing required fields"}, status=400)
        try:
            project = Projects.objects.get(id=project_id)
        except Projects.DoesNotExist:
            return Response({"error": "Project not found"}, status=404)
        file_folder = os.path.join(settings.MEDIA_ROOT, f"user_{project.user.id}/project_{project.id}/{file_type}")
        original_file_path = os.path.join(file_folder, file_name, sheet_name)
        original_file_path = os.path.normpath(original_file_path)
        if not os.path.exists(original_file_path):
            return Response({"error": "File not found in KPI folder"}, status=404)
        file_ext = os.path.splitext(sheet_name)[1].lower()
        file_size_mb = os.path.getsize(original_file_path) / (1024 * 1024)
        try:
            if file_size_mb > 50:
                spark = get_spark_session()
                if file_ext in ['.xlsx', '.xls']:
                    df = spark.read.format("com.crealytics.spark.excel").option("header", True).load(original_file_path)
                elif file_ext == '.csv':
                    df = spark.read.option("header", True).csv(original_file_path)
                else:
                    return Response({"error": "Unsupported file type. Only .csv, .xls, .xlsx allowed."}, status=400)
                mapping_df = pd.read_excel(mapping_file)
                for original_col, mapping_col in json.loads(column_mappings) if isinstance(column_mappings, str) else column_mappings.items():
                    if original_col in df.columns and mapping_col in mapping_df.columns:
                        mapping_spark = spark.createDataFrame(mapping_df[[mapping_col, mapping_df.columns[mapping_df.columns.get_loc(mapping_col)+1]]])
                        mapping_spark = mapping_spark.withColumnRenamed(mapping_col, "key").withColumnRenamed(mapping_df.columns[mapping_df.columns.get_loc(mapping_col)+1], "value")
                        df = df.join(mapping_spark, df[original_col] == mapping_spark["key"], "left") \
                               .withColumn(f"{original_col}_New", expr(f"coalesce(value, {original_col})")) \
                               .drop("key", "value")
                df = df.na.fill("NA")
                df.toPandas().to_csv(original_file_path, index=False)
                result_df = df.limit(50).toPandas()
            else:
                if file_ext in ['.xlsx', '.xls']:
                    df = pd.read_excel(original_file_path)
                elif file_ext == '.csv':
                    df = pd.read_csv(original_file_path)
                else:
                    return Response({"error": "Unsupported file type. Only .csv, .xls, .xlsx allowed."}, status=400)
                mapping_df = pd.read_excel(mapping_file)
                if isinstance(column_mappings, str):
                    try:
                        column_mappings = json.loads(column_mappings)
                    except json.JSONDecodeError:
                        return Response({"error": "Invalid JSON format for column_mappings"}, status=400)
                for original_col, mapping_col in column_mappings.items():
                    try:
                        new_col_name = f"{original_col}_New"
                        if original_col in df.columns and mapping_col in mapping_df.columns:
                            mapping_col_index = mapping_df.columns.get_loc(mapping_col)
                            next_col_index = mapping_col_index + 1
                            if next_col_index < len(mapping_df.columns):
                                next_col_name = mapping_df.columns[next_col_index]
                                mapping_dict = dict(zip(mapping_df[mapping_col], mapping_df[next_col_name]))
                                df[new_col_name] = df[original_col].map(mapping_dict).fillna(df[original_col])
                            else:
                                print(f"Warning: No next column exists after '{mapping_col}' in mapping_df")
                    except Exception as e:
                        return Response({"error": f"Error processing column mapping: {str(e)}"}, status=500)
                df.replace([np.inf, -np.inf], np.nan, inplace=True)
                df.fillna("NA", inplace=True)
                if file_ext in ['.xlsx', '.xls']:
                    df.to_excel(original_file_path, index=False)
                elif file_ext == '.csv':
                    df.to_csv(original_file_path, index=False)
                result_df = df
            self.commit_to_git(
                os.path.join(settings.MEDIA_ROOT, f"user_{project.user.id}/project_{project.id}"),
                project.user,
                project.id,
                file_type,
                file_name,
                sheet_name or ''
            )
            return Response({
                "message": "Mapped and overwritten the original file successfully",
                "mapped_file": {
                    "columns": result_df.columns.tolist(),
                    "data": result_df.values.tolist()
                }
            })
        except Exception as e:
            return Response({"error": f"Unexpected error: {str(e)}"}, status=500)

    def commit_to_git(self, project_folder, user, project_id, file_type, file_name, sheet_name):
        try:
            # Initialize git repo if it doesn't exist
            if not os.path.exists(os.path.join(project_folder, ".git")):
                subprocess.run(["git", "init"], cwd=project_folder)
                subprocess.run(["git", "config", "user.name", user.name], cwd=project_folder)
                subprocess.run(["git", "config", "user.email", user.email], cwd=project_folder)

            # Make relative path to the file/folder
            file_path_relative = os.path.join(file_type, file_name)

            # Stage the changes
            subprocess.run(["git", "add", file_path_relative], cwd=project_folder)

            # Commit message for mapping action
            commit_message = f"mapping - {user.id}/{project_id}/{file_type}/{file_name}/{sheet_name}"
            subprocess.run(["git", "commit", "-m", commit_message], cwd=project_folder)

        except Exception as e:
            print(f"Git commit failed: {str(e)}")



class Melting(APIView):
    def post(self, request):
        project_id = request.data.get("project_id")
        file_name = request.data.get("file_name")
        id_vars = request.data.get("id_vars")
        value_vars = request.data.get("value_vars")
        var_name = request.data.get("var_name", "Variable")
        value_name = request.data.get("value_name", "Value")
        sheet_name = request.data.get("sheet_name")
        file_type = request.data.get("file_type")
        file_name = os.path.basename(file_name)
        if not project_id or not file_name:
            return Response({"error": "Missing required fields"}, status=400)
        try:
            project = Projects.objects.get(id=project_id)
        except Projects.DoesNotExist:
            return Response({"error": "Project not found"}, status=404)
        user = project.user
        project_base_folder = os.path.join(settings.MEDIA_ROOT, f"user_{user.id}/project_{project_id}")
        file_folder = os.path.join(project_base_folder, file_type)
        original_file_path = os.path.join(file_folder, file_name, sheet_name)
        if not os.path.exists(original_file_path):
            return Response({"error": f"Original file not found in {file_type} folder"}, status=404)
        file_size_mb = os.path.getsize(original_file_path) / (1024 * 1024)
        try:
            if file_size_mb > 50:
                spark = get_spark_session()
                df = spark.read.option("header", True).csv(original_file_path)
                # Handle comma-separated input
                if isinstance(id_vars, str):
                    id_vars = [col.strip() for col in id_vars.split(",")]
                if isinstance(value_vars, str):
                    value_vars = [col.strip() for col in value_vars.split(",")]
                n = len(value_vars)
                exprs = ", ".join([f"'{v}', {v}" for v in value_vars])
                melted = df.selectExpr(
                    *id_vars,
                    f"stack({n}, {exprs}) as ({var_name}, {value_name})"
                )
                melted = melted.na.fill("NA")
                melted.toPandas().to_csv(original_file_path, index=False)
                melted_df = melted.limit(50).toPandas()
            else:
                try:
                    df = pd.read_csv(original_file_path)
                except UnicodeDecodeError:
                    df = pd.read_csv(original_file_path, encoding='latin1')
                if isinstance(id_vars, str):
                    id_vars = [col.strip() for col in id_vars.split(",")]
                if isinstance(value_vars, str):
                    value_vars = [col.strip() for col in value_vars.split(",")]
                melted_df = df.melt(id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name)
                melted_df.replace([np.inf, -np.inf], np.nan, inplace=True)
                melted_df.fillna("NA", inplace=True)
                melted_df.to_csv(original_file_path, index=False)
            self.commit_to_git(project_base_folder, user, project_id, file_type, file_name, sheet_name)
            return Response({
                "message": "Melted and overwritten the original file successfully",
                "melted_file": {
                    "columns": melted_df.columns.tolist(),
                    "data": melted_df.values.tolist()
                }
            }, status=200)
        except Exception as e:
            return Response({"error": f"Error during melting: {str(e)}"}, status=500)

    def commit_to_git(self, project_folder, user, project_id, file_type, file_name, sheet_name):
        try:
            if not os.path.exists(os.path.join(project_folder, ".git")):
                subprocess.run(["git", "init"], cwd=project_folder)
                subprocess.run(["git", "config", "user.name", user.name], cwd=project_folder)
                subprocess.run(["git", "config", "user.email", user.email], cwd=project_folder)

            file_path_relative = os.path.join(file_type, file_name)
            subprocess.run(["git", "add", file_path_relative], cwd=project_folder)
            commit_message = f"melted - {user.id}/{project_id}/{file_type}/{file_name}/{sheet_name}"
            subprocess.run(["git", "commit", "-m", commit_message], cwd=project_folder)
        except Exception as e:
            print(f"Git commit failed: {str(e)}")


class SheetInfo(APIView):
    def post(self, request):
        try:
            # Extract payload data
            file_type = request.data.get('file_type')
            file_name = request.data.get('file_name')
            project_id = request.data.get('project_id')
            sheet_name = request.data.get('sheet_name')

            file_name = os.path.basename(file_name)

            if not all([file_type, file_name, project_id, sheet_name]):
                return Response({"error": "Missing required fields"}, status=400)

            # Get project
            try:
                project = Projects.objects.get(id=project_id)
            except Projects.DoesNotExist:
                return Response({"error": "Project not found"}, status=404)

            # Construct file path
            file_path = os.path.join(
                settings.MEDIA_ROOT,
                f"user_{project.user.id}/project_{project.id}/{file_type}/{file_name}/{sheet_name}"
            )
            file_path = os.path.normpath(file_path)

            if not os.path.exists(file_path):
                return Response({"error": "File not found"}, status=404)

            # Read the file
            file_extension = os.path.splitext(sheet_name)[1].lower()
            if file_extension == '.csv':
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='latin1')
            elif file_extension in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                return Response({"error": "Unsupported file format"}, status=400)

            column_info = {}

            for column in df.columns:
                column_data = df[column]
                null_count = column_data.isnull().sum()
                blank_count = (column_data == '').sum() if column_data.dtype == 'object' else 0
                total_empty = null_count + blank_count

                column_info[column] = {
                    "null_count": int(total_empty),
                    "data_type": str(column_data.dtype),
                    "unique_values": int(column_data.nunique()),
                    "unique_elements": column_data.dropna().unique().tolist()[:100]  # limit to 100 unique elements
                }

                # Check if numeric
                try:
                    numeric_data = pd.to_numeric(column_data, errors='coerce')
                    if not numeric_data.isnull().all():
                        column_info[column].update({
                            "type": "numeric",
                            "min": float(numeric_data.min()),
                            "max": float(numeric_data.max()),
                            "average": float(numeric_data.mean()),
                            "unique_values": int(numeric_data.nunique())
                        })
                        continue
                except:
                    pass

                # Check if datetime
                try:
                    date_data = pd.to_datetime(column_data, errors='coerce', dayfirst=False)
                    if not date_data.isnull().all():
                        granularity = 'daily'
                        date_data_clean = date_data.dropna()
                        date_diff = date_data_clean.diff().median()

                        if date_diff.days >= 365:
                            granularity = 'yearly'
                            freq = 'Y'
                            existing_dates = set(date_data_clean.dt.year)
                            expected_dates = set(pd.date_range(start=date_data_clean.min(), end=date_data_clean.max(), freq=freq).year)
                            missing_dates = sorted(expected_dates - existing_dates)
                            missing_formatted = [str(y) for y in missing_dates]
                        elif date_diff.days >= 28:
                            granularity = 'monthly'
                            freq = 'M'
                            existing_dates = set(date_data_clean.dt.to_period('M'))
                            expected_dates = set(pd.date_range(start=date_data_clean.min(), end=date_data_clean.max(), freq=freq).to_period('M'))
                            missing_dates = sorted(expected_dates - existing_dates)
                            missing_formatted = [str(m) for m in missing_dates]
                        else:
                            granularity = 'daily'
                            freq = 'D'
                            existing_dates = set(date_data_clean.dt.normalize())
                            expected_dates = set(pd.date_range(start=date_data_clean.min(), end=date_data_clean.max(), freq=freq))
                            missing_dates = sorted(expected_dates - existing_dates)
                            missing_formatted = [d.strftime('%Y-%m-%d') for d in missing_dates]

                        column_info[column].update({
                            "type": "datetime",
                            "granularity": granularity,
                            "min_date": date_data_clean.min().strftime('%Y-%m-%d %H:%M:%S'),
                            "max_date": date_data_clean.max().strftime('%Y-%m-%d %H:%M:%S'),
                            "missing_dates": missing_formatted
                        })
                        continue
                except:
                    pass

                # Fallback to string
                column_info[column].update({
                    "type": "string",
                    "unique_values": int(column_data.nunique())
                })

            return Response({
                "message": "Column information retrieved successfully",
                "columns": column_info
            }, status=200)

        except Exception as e:
            return Response({"error": f"Error analyzing sheet: {str(e)}"}, status=500)

class CleaningColumns(APIView):
    def post(self, request):
        file_type = request.data.get('file_type')
        file_name = request.data.get('file_name')
        project_id = request.data.get('project_id')
        sheet_name = request.data.get('sheet_name')  # will act as CSV file name here

        print(sheet_name)
        analysis_only = request.data.get('analysis_only', False)
        selected_columns = request.data.get('selected_columns', [])
        options = request.data.get('options', {})

        file_name = os.path.basename(file_name)
        sheet_name = os.path.basename(sheet_name)  # sheet_name = CSV filename now

        if not all([file_type, file_name, project_id, sheet_name]):
            return Response({'error': 'Missing required fields'}, status=400)

        try:
            project = Projects.objects.get(id=project_id)
        except Projects.DoesNotExist:
            return Response({'error': 'Project not found'}, status=404)

        user = project.user
        project_folder = os.path.join(settings.MEDIA_ROOT, f"user_{user.id}/project_{project.id}")
        csv_folder_path = os.path.join(project_folder, file_type, file_name)
        csv_file_path = os.path.join(csv_folder_path, sheet_name)

        if not default_storage.exists(csv_file_path):
            return Response({'error': 'CSV file not found'}, status=404)

        local_path = os.path.join(settings.MEDIA_ROOT, csv_file_path)
        file_size_mb = os.path.getsize(local_path) / (1024 * 1024)

        try:
            if file_size_mb > 50:
                spark = get_spark_session()
                df = spark.read.option("header", True).csv(local_path)
                # Lowercase columns
                for col_name in options.get("lowercase_columns", []):
                    if col_name in df.columns:
                        df = df.withColumn(col_name, lower(col(col_name)))
                # Remove spaces
                for col_name in options.get("remove_spaces_columns", []):
                    if col_name in df.columns:
                        df = df.withColumn(col_name, regexp_replace(col(col_name), " ", ""))
                # Remove special chars
                for col_name, char_map in options.get("remove_special_chars", {}).items():
                    if col_name in df.columns:
                        for char, remove in char_map.items():
                            if remove:
                                df = df.withColumn(col_name, regexp_replace(col(col_name), re.escape(char), ""))
                # Fill NA
                df = df.na.fill("NA")
                # Save back to CSV
                df.toPandas().to_csv(local_path, index=False)
                cleaned_data = df.limit(50).toPandas()  # For preview
                special_chars_analysis = {}  # Not implemented for Spark
            else:
                with default_storage.open(csv_file_path, 'rb') as f:
                    df = pd.read_csv(f)
                special_chars_analysis = self.analyze_special_characters(df)
                if analysis_only:
                    return Response({
                        'status': 'success',
                        'special_chars_analysis': special_chars_analysis,
                        'columns': df.columns.tolist()
                    }, status=200)
                if selected_columns:
                    invalid_columns = [col for col in selected_columns if col not in df.columns]
                    if invalid_columns:
                        return Response({
                            'error': f'Invalid columns: {", ".join(invalid_columns)}',
                            'special_chars_analysis': special_chars_analysis
                        }, status=400)
                cleaned_data = df.copy()
                # Apply cleaning options
                for column in options.get("lowercase_columns", []):
                    if column in cleaned_data.columns:
                        cleaned_data[column] = cleaned_data[column].astype(str).str.lower()
                cleaned_data = self.remove_spaces(cleaned_data, options.get("remove_spaces_columns", []))
                cleaned_data = self.remove_special_characters(cleaned_data, options.get("remove_special_chars", {}))
                cleaned_data = cleaned_data.replace([np.inf, -np.inf], np.nan)
                cleaned_data = cleaned_data.fillna("NA")
                # Save cleaned CSV back
                with default_storage.open(csv_file_path, 'w') as f:
                    cleaned_data.to_csv(f, index=False, lineterminator='\n')
            self.commit_to_git(project_folder, user, project_id, file_type, file_name, sheet_name)
            return Response({
                'message': 'CSV file cleaned successfully',
                'cleaned_sheet': {
                    sheet_name: {
                        'columns': cleaned_data.columns.tolist(),
                        'data': cleaned_data.values.tolist()
                    }
                },
                'special_chars_analysis': special_chars_analysis
            }, status=200)
        except Exception as e:
            return Response({'error': str(e)}, status=500)

    def commit_to_git(self, project_folder, user, project_id, file_type, file_name, sheet_name):
        try:
            if not os.path.exists(os.path.join(project_folder, ".git")):
                subprocess.run(["git", "init"], cwd=project_folder)
                subprocess.run(["git", "config", "user.name", user.name], cwd=project_folder)
                subprocess.run(["git", "config", "user.email", user.email], cwd=project_folder)

            subprocess.run(["git", "add", "."], cwd=project_folder)
            commit_message = f"cleaned - {user.id}/{project_id}/{file_type}/{file_name}/{sheet_name}"
            subprocess.run(["git", "commit", "-m", commit_message], cwd=project_folder)
        except Exception as e:
            print(f"Git commit failed: {str(e)}")

    def analyze_special_characters(self, df):
        special_chars_analysis = {}
        for column in df.columns:
            values = df[column].astype(str).fillna('')
            special_chars = set(re.findall(r'[^a-zA-Z0-9]', ' '.join(values)))
            char_stats = {}
            total_char_count = 0
            for char in special_chars:
                if char == ' ':
                    continue
                row_indices = [i for i, value in enumerate(values) if char in value]
                count_per_row = [value.count(char) for value in values if char in value]
                total_char_count += sum(count_per_row)
                char_stats[char] = {
                    'total_occurrences': sum(count_per_row),
                    'rows_containing': len(row_indices),
                    'min_row_index': min(row_indices) if row_indices else None,
                    'max_row_index': max(row_indices) if row_indices else None,
                    'avg_occurrences_per_row': round(sum(count_per_row) / len(row_indices), 2) if row_indices else 0
                }
            blank_cells_count = df[column].isnull().sum()
            special_chars_analysis[column] = {
                'special_characters': char_stats,
                'blank_cells_count': blank_cells_count,
                'total_special_char_occurrences': total_char_count
            }
        return special_chars_analysis

    def remove_spaces(self, df, columns):
        for column in columns:
            if column in df.columns:
                df[column] = df[column].astype(str).str.replace(" ", "", regex=True)
        return df

    def remove_special_characters(self, df, options):
        for column, char_map in options.items():
            if column in df.columns:
                for char, remove in char_map.items():
                    if remove:
                        df[column] = df[column].astype(str).str.replace(re.escape(char), "", regex=True)
        return df


class FileUploadView(APIView):
    def post(self, request):
        file_type = request.data.get('file_type')
        file_name = request.data.get('file_name')
        project_id = request.data.get('project_id')

        file_name = os.path.basename(file_name)

        # Validate required fields
        if not file_name or not project_id or not file_type:
            return Response({'error': 'Missing required fields'}, status=400)

        # Ensure project exists
        try:
            project = Projects.objects.get(id=project_id)
        except Projects.DoesNotExist:
            return Response({'error': 'Project not found'}, status=404)

        # Define folder structure
        project_folder = f"user_{project.user.id}/project_{project.id}"
        last_name_kpi = [os.path.basename(file) for file in project.kpi_file] if project.kpi_file else []
        last_name_media = [os.path.basename(file) for file in project.media_file] if project.media_file else []

        # Build path based on file type
        if file_name in last_name_kpi and file_type == 'kpi':
            base_folder = os.path.join(settings.MEDIA_ROOT, project_folder, "kpi", file_name)
        elif file_name in last_name_media and file_type == 'media':
            base_folder = os.path.join(settings.MEDIA_ROOT, project_folder, "media", file_name)
        else:
            return Response({'error': 'File not associated with the project'}, status=400)

        base_folder = os.path.normpath(base_folder)

        if not os.path.exists(base_folder):
            return Response({'error': 'Folder not found'}, status=404)

        # Collect all CSV files inside the folder
        csv_files = []
        for root, dirs, files in os.walk(base_folder):
            for file in files:
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(root, file))

        if not csv_files:
            return Response({'error': 'No CSV files found inside the folder'}, status=404)

        sheets_data = {}

        try:
            for csv_file in csv_files:
                # Relativize the path for Django's default_storage
                rel_csv_file = os.path.relpath(csv_file, settings.MEDIA_ROOT)

                # Determine file size
                file_size_mb = os.path.getsize(csv_file) / (1024 * 1024)

                # Open CSV file using default_storage in binary mode, wrap in TextIOWrapper for utf-8
                with default_storage.open(rel_csv_file, 'rb') as f:
                    csvfile = io.TextIOWrapper(f, encoding='utf-8')
                    reader = csv.reader(csvfile)
                    rows = list(reader)

                    if not rows:
                        continue  # Skip empty CSV files

                    columns = rows[0]  # Header
                    if file_size_mb < 50:
                        data = rows[1:]  # All data rows
                    else:
                        data = rows[1:1001]  # Top 1000 rows

                    column_types = {}

                    # Common error values to ignore
                    error_values = {"#VALUE!", "#N/A", "#DIV/0!", "#REF!", "#NAME?", "#NULL!", "#NUM!"}

                    for col_index, col_name in enumerate(columns):
                        column_types[col_name] = "Unknown"
                        for row in data:
                            try:
                                value = row[col_index]
                                if value and value not in error_values:
                                    if value.replace('.', '', 1).isdigit():
                                        if '.' in value:
                                            column_types[col_name] = "float"
                                        else:
                                            column_types[col_name] = "int"
                                    else:
                                        column_types[col_name] = "str"
                                    break
                            except (IndexError, ValueError, TypeError):
                                continue

                    # Use CSV file name as "sheet name"
                    sheet_name = os.path.basename(csv_file)
                    sheets_data[sheet_name] = {
                        'columns': columns,
                        'data': data,
                        'column_types': column_types,
                        'hidden': False  # CSV can't have hidden sheets
                    }

            return Response({
                'message': 'CSV file(s) data retrieved successfully',
                'sheets_data': sheets_data
            }, status=200)

        except Exception as e:
            return Response({'error': f'Error reading the CSV files: {str(e)}'}, status=500)
        


class SignupView(APIView):
    def post(self, request):
        username = request.data.get('username')
        password = request.data.get('password')
        email = request.data.get('email')

        if not username or not password or not email:
            return Response({'error': 'Missing required fields'}, status=400)

        if User.objects.filter(username=username).exists():
            return Response({'error': 'Username already taken'}, status=400)

        user = User.objects.create(username=username, email=email, password=password)
        refresh = RefreshToken.for_user(user)
        return Response({
            'user': {
                'id': user.id,
                'username': user.username,
                'email': user.email
            },
            'refresh': str(refresh),
            'access': str(refresh.access_token),
        }, status=201)


class SigninView(APIView):
    def post(self, request):
        username = request.data.get('username')
        password = request.data.get('password')
        # print(username, password)
        user = CustomAuthBackend.authenticate(username=username, password=password)
        # print(user)
        if user:
            refresh = RefreshToken.for_user(user)
            return Response({
                'user': {
                    'id': user.id,
                    'username': user.username,
                    'email': user.email
                },
                'refresh': str(refresh),
                'access': str(refresh.access_token),
            })
        return Response({'error': 'Invalid credentials'}, status=401)
    

class UploadProject(APIView):
    def post(self, request):
        user_id = request.data.get('user_id')
        name = request.data.get('project_name')
        kpi_files = request.FILES.getlist('kpi_file')
        media_files = request.FILES.getlist('media_file')

        if not user_id or not name:
            return Response({'error': 'Missing required fields'}, status=400)

        if not kpi_files and not media_files:
            return Response({'error': 'No files provided for KPI or Media'}, status=400)

        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=404)

        # Create new project
        project = Projects.objects.create(user=user, name=name, kpi_file=[], media_file=[])
        project_folder = os.path.join(settings.MEDIA_ROOT, f"user_{user.id}/project_{project.id}")
        os.makedirs(project_folder, exist_ok=True)

        if not os.path.exists(os.path.join(project_folder, ".git")):
            subprocess.run(["git", "init"], cwd=project_folder)
            subprocess.run(["git", "config", "user.name", user.username], cwd=project_folder)
            subprocess.run(["git", "config", "user.email", user.email], cwd=project_folder)

        spark = None  # Spark session, only created if needed
        spark_needed = False
        all_files = [(file, 'kpi') for file in kpi_files] + [(file, 'media') for file in media_files]
        # Check if any file is >= 50MB
        for file, _ in all_files:
            file_basename = os.path.splitext(os.path.basename(file.name))[0]
            temp_path = os.path.join(project_folder, 'kpi' if file in kpi_files else 'media', file_basename, file.name)
            os.makedirs(os.path.dirname(temp_path), exist_ok=True)
            with open(temp_path, 'wb') as f_out:
                for chunk in file.chunks():
                    f_out.write(chunk)
            file_size_mb = os.path.getsize(temp_path) / (1024 * 1024)
            if file_size_mb >= 50:
                spark_needed = True
            # Remove the temp file after checking size, will be re-written in process_excel_file
            os.remove(temp_path)
        if spark_needed:
            spark = get_spark_session()

        def process_excel_file(file, file_type):
            if file_type == 'kpi':
                last_obj = Projects.objects.order_by('-kpi_id').first()
                id_field = 'kpi_id'
                updated_list = project.kpi_file
                base_subdir = 'kpi'
            else:
                last_obj = Projects.objects.order_by('-media_id').first()
                id_field = 'media_id'
                updated_list = project.media_file
                base_subdir = 'media'

            file_id = (getattr(last_obj, id_field, 0) or 0) + 1
            file_basename = os.path.splitext(os.path.basename(file.name))[0]
            file_folder = os.path.join(project_folder, base_subdir, file_basename)
            os.makedirs(file_folder, exist_ok=True)

            temp_path = os.path.join(file_folder, file.name)
            with open(temp_path, 'wb') as f:
                for chunk in file.chunks():
                    f.write(chunk)

            file_size_mb = os.path.getsize(temp_path) / (1024 * 1024)

            if file_size_mb < 50:
                with pd.ExcelFile(temp_path) as xls:
                    for sheet_name in xls.sheet_names:
                        df = xls.parse(sheet_name, dtype=str)
                        sheet_path = os.path.join(file_folder, f"{sheet_name}.csv")
                        df.to_csv(sheet_path, index=False)
                        commit_msg = f"uploaded - {user.id}/{project.id}/{base_subdir}/{file_id}/{file_basename}/{sheet_name}"
                        subprocess.run(["git", "add", sheet_path], cwd=project_folder)
                        subprocess.run(["git", "commit", "-m", commit_msg], cwd=project_folder)
            else:
                if spark is None:
                    raise Exception("Spark session was not initialized for large file upload.")
                try:
                    xls = pd.ExcelFile(temp_path, engine='openpyxl')
                    sheet_names = xls.sheet_names
                except Exception as e:
                    print(f"❌ Failed to extract sheet names: {e}")
                    raise
                def convert_sheet(sheet_name):
                    output_path = os.path.join(file_folder, f"{sheet_name}.csv")
                    commit_msg = f"uploaded - {user.id}/{project.id}/{base_subdir}/{file_id}/{file_basename}/{sheet_name}"
                    try:
                        df = spark.read \
                            .format("com.crealytics.spark.excel") \
                            .option("dataAddress", f"'{sheet_name}'!A1") \
                            .option("header", "true") \
                            .option("inferSchema", "false") \
                            .option("maxRowsInMemory", 10000) \
                            .option("maxColumns", 10000) \
                            .option("treatEmptyValuesAsNulls", "true") \
                            .option("workbookPassword", None) \
                            .load(temp_path)
                        df.toPandas().to_csv(output_path, index=False)
                        subprocess.run(["git", "add", output_path], cwd=project_folder)
                        subprocess.run(["git", "commit", "-m", commit_msg], cwd=project_folder)
                    except Exception as e:
                        if "RecordFormatException" in str(e):
                            import warnings
                            warnings.filterwarnings("ignore", category=UserWarning)
                            try:
                                xls = pd.ExcelFile(temp_path, engine='openpyxl')
                                df = xls.parse(sheet_name, dtype=str)
                                df.to_csv(output_path, index=False)
                                subprocess.run(["git", "add", output_path], cwd=project_folder)
                                subprocess.run(["git", "commit", "-m", commit_msg], cwd=project_folder)
                                print(f"✅ Fallback to pandas successful for sheet: {sheet_name}")
                            except Exception as pe:
                                print(f"❌ Pandas fallback failed for {sheet_name}: {pe}")
                        else:
                            print(f"❌ Unexpected Spark error for {sheet_name}: {e}")
                            raise
                with ThreadPoolExecutor(max_workers=min(4, len(sheet_names))) as executor:
                    futures = [executor.submit(convert_sheet, sheet) for sheet in sheet_names]
                    for f in futures:
                        f.result()
            updated_list.append(file_basename)
            setattr(project, id_field, file_id)

        updated_kpi_files = []
        for file in kpi_files:
            process_excel_file(file, 'kpi')
            updated_kpi_files.append(os.path.splitext(file.name)[0])

        updated_media_files = []
        for file in media_files:
            process_excel_file(file, 'media')
            updated_media_files.append(os.path.splitext(file.name)[0])

        project.save()

        if spark:
            spark.stop()

        return Response({
            'message': 'Project uploaded successfully',
            'project_id': project.id,
            'name': project.name,
            'uploaded_kpi_files': updated_kpi_files,
            'uploaded_media_files': updated_media_files,
        }, status=201)



class UserProjectsView(APIView):
    def post(self, request):
        user_id = request.data.get('user_id')

        if not user_id:
            return Response({'error': 'User ID is required'}, status=400)

        user = get_object_or_404(User, id=user_id)
        projects = Projects.objects.filter(user=user)

        project_data = []
        for project in projects:
            # Ensure kpi_file and media_file are lists
            kpi_files = project.kpi_file if isinstance(project.kpi_file, list) else []
            media_files = project.media_file if isinstance(project.media_file, list) else []

            project_data.append({
                'id': project.id,
                'name': project.name,
                'files': {
                    'kpis': [{'id': idx + 1, 'path': f"user_{user.id}/project_{project.id}/kpi/{file_name}", 'name': file_name} 
                             for idx, file_name in enumerate(kpi_files)],
                    'media': [{'id': idx + 1, 'path': f"user_{user.id}/project_{project.id}/media/{file_name}", 'name': file_name} 
                              for idx, file_name in enumerate(media_files)],
                }
            })

        return Response({'projects': project_data}, status=200)


class DeleteProject(APIView):
    def delete(self, request):
        project_id = request.data.get("project_id")
        

        if not project_id :
            return Response({"error": "Project ID and User ID are required"}, status=400)

        try:
            project = Projects.objects.get(id=project_id)
        except Projects.DoesNotExist:
            return Response({"error": "Project not found"}, status=404)

        # Define project folder path
        project_path = os.path.join(settings.MEDIA_ROOT, f"user_{project.user.id}/project_{project_id}")
        project_path = os.path.abspath(os.path.normpath(project_path))

        print("Deleting project at:", project_path)

        try:
            # Check if it's a Git repository
            git_folder = os.path.join(project_path, ".git")
            if os.path.exists(git_folder):
                print("Removing Git repository...")

                # Run git commands to clean up the repository
                subprocess.run(["git", "-C", project_path, "gc", "--prune=now"], check=False)
                subprocess.run(["git", "-C", project_path, "rm", "-rf", "--cached", "."], check=False)

                # Force remove .git folder
                shutil.rmtree(git_folder, ignore_errors=True)

            # Now delete the project folder
            if os.path.exists(project_path):
                def remove_readonly(func, path, _):
                    """Force remove read-only files like .git/index.lock"""
                    os.chmod(path, stat.S_IWRITE)
                    func(path)

                shutil.rmtree(project_path, onerror=remove_readonly)

            # Delete the project from the database
            project.delete()

            return Response({"message": "Project, files, and Git repository deleted successfully"}, status=200)

        except Exception as e:
            print("Error while deleting project:", traceback.format_exc())  # Print full error details
            return Response({"error": str(e)}, status=500)
        


class UpdateProject(APIView):
    def post(self, request):
        project_id = request.data.get('project_id')
        user_id = request.data.get('user_id')
        kpi_files = request.FILES.getlist('kpi_file')
        media_files = request.FILES.getlist('media_file')

        if not project_id or not user_id:
            return Response({'error': 'Missing required fields'}, status=400)

        try:
            project = Projects.objects.get(id=project_id, user_id=user_id)
        except Projects.DoesNotExist:
            return Response({'error': 'Project not found or access denied'}, status=404)

        if not kpi_files and not media_files:
            return Response({'error': 'No files provided for update'}, status=400)

        project_folder = os.path.join(settings.MEDIA_ROOT, f"user_{user_id}/project_{project_id}")
        os.makedirs(project_folder, exist_ok=True)

        try:
            user = User.objects.get(id=user_id)
        except User.DoesNotExist:
            return Response({'error': 'User not found'}, status=404)

        if not os.path.exists(os.path.join(project_folder, ".git")):
            subprocess.run(["git", "init"], cwd=project_folder)
            subprocess.run(["git", "config", "user.name", user.username], cwd=project_folder)
            subprocess.run(["git", "config", "user.email", user.email], cwd=project_folder)

        spark = None  # Spark session, only created if needed
        spark_needed = False
        all_files = [(file, 'kpi') for file in kpi_files] + [(file, 'media') for file in media_files]
        # Check if any file is >= 50MB
        for file, _ in all_files:
            file_basename = os.path.splitext(os.path.basename(file.name))[0]
            temp_path = os.path.join(project_folder, 'kpi' if file in kpi_files else 'media', file_basename, file.name)
            os.makedirs(os.path.dirname(temp_path), exist_ok=True)
            with open(temp_path, 'wb') as f_out:
                for chunk in file.chunks():
                    f_out.write(chunk)
            file_size_mb = os.path.getsize(temp_path) / (1024 * 1024)
            if file_size_mb >= 50:
                spark_needed = True
            # Remove the temp file after checking size, will be re-written in process_excel_file
            os.remove(temp_path)
        if spark_needed:
            spark = get_spark_session()

        def process_excel_file(file, file_type):
            if file_type == 'kpi':
                last_obj = Projects.objects.order_by('-kpi_id').first()
                id_field = 'kpi_id'
                updated_list = project.kpi_file
                base_subdir = 'kpi'
            else:
                last_obj = Projects.objects.order_by('-media_id').first()
                id_field = 'media_id'
                updated_list = project.media_file
                base_subdir = 'media'

            file_id = (getattr(last_obj, id_field, 0) or 0) + 1
            file_basename = os.path.splitext(os.path.basename(file.name))[0]
            file_folder = os.path.join(project_folder, base_subdir, file_basename)
            os.makedirs(file_folder, exist_ok=True)

            temp_path = os.path.join(file_folder, file.name)
            with open(temp_path, 'wb') as f:
                for chunk in file.chunks():
                    f.write(chunk)

            file_size_mb = os.path.getsize(temp_path) / (1024 * 1024)

            if file_size_mb < 50:
                with pd.ExcelFile(temp_path) as xls:
                    for sheet_name in xls.sheet_names:
                        df = xls.parse(sheet_name, dtype=str)
                        sheet_path = os.path.join(file_folder, f"{sheet_name}.csv")
                        df.to_csv(sheet_path, index=False)
                        commit_msg = f"updated - {user_id}/{project_id}/{base_subdir}/{file_basename}/{sheet_name}"
                        subprocess.run(["git", "add", sheet_path], cwd=project_folder)
                        subprocess.run(["git", "commit", "-m", commit_msg], cwd=project_folder)
            else:
                if spark is None:
                    raise Exception("Spark session was not initialized for large file update.")
                try:
                    xls = pd.ExcelFile(temp_path, engine='openpyxl')
                    sheet_names = xls.sheet_names
                except Exception as e:
                    print(f"❌ Failed to extract sheet names: {e}")
                    raise
                def convert_sheet(sheet_name):
                    output_path = os.path.join(file_folder, f"{sheet_name}.csv")
                    commit_msg = f"updated - {user_id}/{project_id}/{base_subdir}/{file_basename}/{sheet_name}"
                    try:
                        df = spark.read \
                            .format("com.crealytics.spark.excel") \
                            .option("dataAddress", f"'{sheet_name}'!A1") \
                            .option("header", "true") \
                            .option("inferSchema", "false") \
                            .option("maxRowsInMemory", 10000) \
                            .option("maxColumns", 10000) \
                            .option("treatEmptyValuesAsNulls", "true") \
                            .option("workbookPassword", None) \
                            .load(temp_path)
                        df.toPandas().to_csv(output_path, index=False)
                        subprocess.run(["git", "add", output_path], cwd=project_folder)
                        subprocess.run(["git", "commit", "-m", commit_msg], cwd=project_folder)
                    except Exception as e:
                        if "RecordFormatException" in str(e):
                            import warnings
                            warnings.filterwarnings("ignore", category=UserWarning)
                            try:
                                xls = pd.ExcelFile(temp_path, engine='openpyxl')
                                df = xls.parse(sheet_name, dtype=str)
                                df.to_csv(output_path, index=False)
                                subprocess.run(["git", "add", output_path], cwd=project_folder)
                                subprocess.run(["git", "commit", "-m", commit_msg], cwd=project_folder)
                                print(f"✅ Fallback to pandas successful for sheet: {sheet_name}")
                            except Exception as pe:
                                print(f"❌ Pandas fallback failed for {sheet_name}: {pe}")
                        else:
                            print(f"❌ Unexpected Spark error for {sheet_name}: {e}")
                            raise
                with ThreadPoolExecutor(max_workers=min(4, len(sheet_names))) as executor:
                    futures = [executor.submit(convert_sheet, sheet) for sheet in sheet_names]
                    for f in futures:
                        f.result()
            updated_list.append(file_basename)
            setattr(project, id_field, file_id)

        updated_kpi_files = []
        for file in kpi_files:
            process_excel_file(file, 'kpi')
            updated_kpi_files.append(os.path.splitext(file.name)[0])

        updated_media_files = []
        for file in media_files:
            process_excel_file(file, 'media')
            updated_media_files.append(os.path.splitext(file.name)[0])

        project.save()

        if spark:
            spark.stop()

        return Response({
            'message': 'Project files updated successfully',
            'project_id': project.id,
            'updated_kpi_files': updated_kpi_files,
            'updated_media_files': updated_media_files,
        }, status=200)


class GetSpecificSheetCommits(APIView):
    def post(self, request):
        project_id = request.data.get("project_id")
        user_id = request.data.get("user_id")
        file_name = request.data.get("file_name")
        sheet_name = request.data.get("sheet_name")
        file_type = request.data.get("file_type")
        send_only_commits = request.data.get("send_only_commits")

        file_name = file_name.replace("\\", "/").split("/")[-1]

        if not project_id or not user_id or not file_name or not sheet_name:
            return Response({"error": "Missing required parameters"}, status=400)

        project_folder = os.path.join(settings.MEDIA_ROOT, f"user_{user_id}/project_{project_id}")
        git_relative_path = f"{file_type}/{file_name}/{sheet_name}"

        try:
            result = subprocess.run(
                ["git", "log", "--oneline", "--reverse", "--", git_relative_path],
                cwd=project_folder,
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                return Response({"error": "Failed to fetch commits"}, status=500)

            raw_commits = result.stdout.strip().split("\n")
            commit_map = {}
            parent_stack = []
            current_branch_parents = []
            last_valid_commit = None

            for i, raw_commit in enumerate(raw_commits):
                parts = raw_commit.split(" ", 1)
                if len(parts) < 2:
                    continue

                commit_hash, message = parts
                message_lower = message.lower()

                # Identify operation
                if "undo" in message_lower:
                    operation_type = "undo"
                elif "redo" in message_lower:
                    operation_type = "redo"
                elif "cleaned" in message_lower:
                    operation_type = "cleaning"
                elif "melted" in message_lower:
                    operation_type = "melting"
                elif "mapping" in message_lower:
                    operation_type = "mapping"
                elif "merging" in message_lower:
                    operation_type = "merging"
                elif "uploaded" in message_lower:
                    operation_type = "uploaded"
                else:
                    operation_type = "other"

                parents = []
                if operation_type == "uploaded":
                    parent_stack = [commit_hash]
                elif operation_type == "undo":
                    if parent_stack:
                        last_valid_commit = parent_stack.pop()
                else:
                    if operation_type == "redo" and last_valid_commit:
                        parents = [last_valid_commit]
                    elif parent_stack:
                        parents = [parent_stack[-1]]
                    else:
                        parents = []

                    parent_stack.append(commit_hash)

                # Retrieve sheet data
                if send_only_commits:
                    sheet_data = None
                else:
                    try:
                        sheet_data_result = subprocess.run(
                            ['git', 'show', f'{commit_hash}:{git_relative_path}'],
                            cwd=project_folder,
                            capture_output=True,
                            text=True,
                            timeout=10
                        )

                        if sheet_data_result.returncode == 0 and sheet_data_result.stdout.strip():
                            from io import StringIO
                            sheet_data_df = pd.read_csv(StringIO(sheet_data_result.stdout.strip()), dtype=str)
                            sheet_data_df = sheet_data_df.replace([np.nan, np.inf, -np.inf], "value error")
                            sheet_data = {
                                "columns": sheet_data_df.columns.tolist(),
                                "data": sheet_data_df.values.tolist()
                            }
                        else:
                            sheet_data = None
                    except Exception as e:
                        print(f"Error retrieving sheet data: {e}")
                        sheet_data = None

                commit_map[commit_hash] = {
                    "hash": commit_hash,
                    "message": message,
                    "operation_type": operation_type,
                    "parents": parents,
                    "children": [],
                    "sheet_data": sheet_data
                }

            # Set up children for each commit
            for commit in commit_map.values():
                for parent_hash in commit["parents"]:
                    if parent_hash in commit_map:
                        commit_map[parent_hash]["children"].append(commit["hash"])

            return Response({"commits": list(commit_map.values())}, status=200)

        except Exception as e:
            return Response({"error": str(e)}, status=500)


# for undo redo purpose without the entry of undo/redo in commits array of response 

class GetSpecificSheetCommitsArray(APIView):
    def post(self, request):
        
        project_id = request.data.get("project_id")
        user_id = request.data.get("user_id")
        file_name = request.data.get("file_name")
        sheet_name = request.data.get("sheet_name")
        file_type = request.data.get("file_type")
        send_only_commits = request.data.get("send_only_commits")

        file_name = file_name.replace("\\", "/").split("/")[-1]

        if not project_id or not user_id or not file_name or not sheet_name:
            return Response({"error": "Missing required parameters"}, status=400)

        project_folder = os.path.join(settings.MEDIA_ROOT, f"user_{user_id}/project_{project_id}")
        git_relative_path = f"{file_type}/{file_name}/{sheet_name}"

        try:
            result = subprocess.run(
                ["git", "log", "--oneline", "--reverse", "--", git_relative_path],
                cwd=project_folder,
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                return Response({"error": "Failed to fetch commits"}, status=500)

            raw_commits = result.stdout.strip().split("\n")
            commit_map = {}
            parent_stack = []
            current_branch_parents = []
            last_valid_commit = None

            for i, raw_commit in enumerate(raw_commits):
                parts = raw_commit.split(" ", 1)
                if len(parts) < 2:
                    continue

                commit_hash, message = parts
                message_lower = message.lower()

                # Identify operation
                if "undo" in message_lower:
                    operation_type = "undo"
                elif "redo" in message_lower:
                    operation_type = "redo"
                elif "cleaned" in message_lower:
                    operation_type = "cleaning"
                elif "melted" in message_lower:
                    operation_type = "melting"
                elif "mapped" in message_lower:
                    operation_type = "mapping"
                elif "merged" in message_lower:
                    operation_type = "merging"
                elif "uploaded" in message_lower:
                    operation_type = "uploaded"
                else:
                    operation_type = "other"

                parents = []
                if operation_type == "uploaded":
                    parent_stack = [commit_hash]
                elif operation_type == "undo":
                    if parent_stack:
                        last_valid_commit = parent_stack.pop()
                else:
                    if operation_type == "redo" and last_valid_commit:
                        parents = [last_valid_commit]
                    elif parent_stack:
                        parents = [parent_stack[-1]]
                    else:
                        parents = []

                    parent_stack.append(commit_hash)

                # Retrieve sheet data
                if send_only_commits:
                    sheet_data = None
                else:
                    try:
                        sheet_data_result = subprocess.run(
                            ['git', 'show', f'{commit_hash}:{git_relative_path}'],
                            cwd=project_folder,
                            capture_output=True,
                            text=True,
                            timeout=10
                        )

                        if sheet_data_result.returncode == 0 and sheet_data_result.stdout.strip():
                            from io import StringIO
                            sheet_data_df = pd.read_csv(StringIO(sheet_data_result.stdout.strip()), dtype=str)
                            sheet_data_df = sheet_data_df.replace([np.nan, np.inf, -np.inf], "value error")
                            sheet_data = {
                                "columns": sheet_data_df.columns.tolist(),
                                "data": sheet_data_df.values.tolist()
                            }
                        else:
                            sheet_data = None
                    except Exception as e:
                        print(f"Error retrieving sheet data: {e}")
                        sheet_data = None

                commit_map[commit_hash] = {
                    "hash": commit_hash,
                    "message": message,
                    "operation_type": operation_type,
                    "parents": parents,
                    "children": [],
                    "sheet_data": sheet_data
                }

            # Set up children for each commit
            
            for commit in commit_map.values():
                for parent_hash in commit["parents"]:
                    if parent_hash in commit_map:
                        commit_map[parent_hash]["children"].append(commit["hash"])

            # Exclude undo and redo commits from the response
            filtered_commits = [commit for commit in commit_map.values() if commit["operation_type"] not in ["undo", "redo"]]

            return Response({"commits": filtered_commits}, status=200)

        except Exception as e:
            return Response({"error": str(e)}, status=500)


class UndoRedoSheet(APIView):
    def post(self, request):
        project_id = request.data.get("project_id")
        user_id = request.data.get("user_id")
        file_name = request.data.get("file_name")
        sheet_name = request.data.get("sheet_name")
        file_type = request.data.get("file_type")
        action = request.data.get("action")  # 'undo' or 'redo'
        hash = request.data.get("hash")

        file_name=file_name.split("\\")[1]

        if not all([project_id, user_id, file_name, sheet_name, file_type, action]):
            return Response({"error": "Missing required parameters"}, status=400)

        project_folder = os.path.join(settings.MEDIA_ROOT, f"user_{user_id}/project_{project_id}")
        sheet_path = os.path.join(project_folder, file_type, file_name, f"{sheet_name}")

        sheet_path=os.path.normpath(sheet_path)

        if not os.path.exists(sheet_path):
            return Response({"error": "Sheet file not found"}, status=404)

        try:
            print("1")

            if isinstance(hash, str):
                hash = json.loads(hash)
            commit_hash = hash.get("hash")
            # Get commit history for the specific sheet
            result = subprocess.run(
                ["git", "log", "--oneline", "--", sheet_path],
                cwd=project_folder,
                capture_output=True,
                text=True
            )

            print("2")

            if result.returncode != 0:
                return Response({"error": "Failed to fetch commit history"}, status=500)

            commits = result.stdout.strip().split("\n")
            if not commits or len(commits) < 2:
                return Response({"error": "No previous state available for undo/redo"}, status=400)
            
            print("3")

            commit_hashes = [commit.split(" ")[0] for commit in commits]
            
            target_index = commit_hashes.index(commit_hash)
            target_commit = commit_hashes[target_index]
            
            print("4")

            if not target_commit:
                return Response({"error": "No commit found for undo/redo"}, status=400)

            # Restore the sheet file from the selected commit
            subprocess.run(["git", "checkout", target_commit, "--", sheet_path], cwd=project_folder)

            df_restored = pd.read_csv(sheet_path)

            df_restored = df_restored.replace([float("inf"), -float("inf")], None)
            df_restored = df_restored.fillna("NA")
            try:
                project = Projects.objects.get(id=project_id)
                self.commit_to_git(
                    project_folder,
                    project.user,
                    project_id,
                    file_type,
                    file_name,
                    sheet_name,
                    action
                )
            except Exception as e:
                print(f"Failed to create commit: {str(e)}")
            return Response({
                "message": f"{action.capitalize()} successful",
                "sheet_data": {
                    "columns": df_restored.columns.tolist(),
                    "data": df_restored.values.tolist()
                }
            }, status=200)

        except Exception as e:
            return Response({"error": str(e)}, status=500)
        
    def commit_to_git(self, project_folder, user, project_id, file_type, file_name, sheet_name, action):
        # Dummy commit logic — update based on your implementation
        commit_msg = f"{action.capitalize()} action on {sheet_name} by {user.username}"
        subprocess.run(["git", "add", "."], cwd=project_folder)
        subprocess.run(["git", "commit", "-m", commit_msg], cwd=project_folder)


class EnhancedEDAAnalysis(APIView):
    def post(self, request):
        try:
            file_type = request.data.get('file_type')
            file_name = request.data.get('file_name')
            project_id = request.data.get('project_id')
            sheet_name = request.data.get('sheet_name')
            analysis_types = request.data.get('analysis_types', ['all'])
            target_column = request.data.get('target_column')
            categorical_columns = request.data.get('categorical_columns', [])
            numerical_columns = request.data.get('numerical_columns', [])
            datetime_columns = request.data.get('datetime_columns', [])

            file_name = os.path.basename(file_name)

            if not all([file_type, file_name, project_id, sheet_name]):
                return Response({"error": "Missing required fields"}, status=400)

            try:
                project = Projects.objects.get(id=project_id)
                file_path = os.path.join(
                    settings.MEDIA_ROOT,
                    f"user_{project.user.id}/project_{project.id}/{file_type}/{file_name}/{sheet_name}"
                )
                file_path = os.path.normpath(file_path)

                if not os.path.exists(file_path):
                    return Response({"error": "File not found"}, status=404)

                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                if file_size_mb > 50:
                    spark = get_spark_session()
                    df = spark.read.option("header", True).csv(file_path)
                    total_rows = df.count()
                    total_columns = len(df.columns)
                    duplicate_rows = total_rows - df.dropDuplicates().count()
                    column_types = {}
                    for col_name, dtype in df.dtypes:
                        if dtype in ['int', 'double', 'float', 'bigint']:
                            column_types[col_name] = 'numerical'
                        elif dtype in ['date', 'timestamp']:
                            column_types[col_name] = 'datetime'
                        else:
                            column_types[col_name] = 'categorical'
                    missing_values = {}
                    for col_name in df.columns:
                        missing_count = df.filter(df[col_name].isNull() | (df[col_name] == "")).count()
                        missing_values[col_name] = {
                            "missing_count": int(missing_count),
                            "missing_percentage": float(missing_count / total_rows * 100) if total_rows else 0
                        }
                    numerical_stats = {}
                    num_cols = [col for col, typ in column_types.items() if typ == 'numerical']
                    for col in num_cols:
                        stats = df.selectExpr(
                            f"mean({col}) as mean",
                            f"percentile({col}, 0.5) as median",
                            f"stddev({col}) as std",
                            f"min({col}) as min",
                            f"max({col}) as max"
                        ).toPandas().to_dict(orient='records')[0]
                        numerical_stats[col] = {k: float(v) if v is not None else None for k, v in stats.items()}
                    categorical_stats = {}
                    cat_cols = [col for col, typ in column_types.items() if typ == 'categorical']
                    for col in cat_cols:
                        value_counts = df.groupBy(col).count().orderBy('count', ascending=False).limit(10).toPandas()
                        categorical_stats[col] = {
                            "unique_values": int(df.select(col).distinct().count()),
                            "top_categories": {str(row[col]): int(row['count']) for _, row in value_counts.iterrows()}
                        }
                    correlation = {}
                    if len(num_cols) > 1:
                        for i, col1 in enumerate(num_cols):
                            correlation[col1] = {}
                            for col2 in num_cols[i+1:]:
                                corr_val = df.stat.corr(col1, col2)
                                correlation[col1][col2] = corr_val
                    eda_results = {
                        "basic_info": {
                            "total_rows": total_rows,
                            "total_columns": total_columns,
                            "duplicate_rows": duplicate_rows,
                            "column_types": column_types
                        },
                        "missing_values": missing_values,
                        "numerical_stats": numerical_stats,
                        "categorical_stats": categorical_stats,
                        "correlation": correlation
                    }
                    spark.stop()
                else:
                    df = pd.read_csv(file_path)
                    eda_results = {
                        "basic_info": {
                            "total_rows": len(df),
                            "total_columns": len(df.columns),
                            "memory_usage": df.memory_usage(deep=True).sum(),
                            "duplicate_rows": df.duplicated().sum(),
                            "column_types": {
                                col: (
                                    "numerical" if pd.api.types.is_numeric_dtype(df[col])
                                    else "datetime" if pd.api.types.is_datetime64_any_dtype(df[col])
                                    else "categorical"
                                )
                                for col in df.columns
                            }
                        },
                        "missing_values": self.analyze_missing_values(df),
                        "numerical_stats": self.analyze_numerical_data(df),
                        "categorical_stats": self.analyze_categorical_data(df),
                        "correlation": self.analyze_correlations(df),
                    }
                return Response({"eda_results": eda_results}, status=200)
            except Projects.DoesNotExist:
                return Response({"error": "Project not found"}, status=404)
        except Exception as e:
            return Response({"error": f"Error during EDA: {str(e)}"}, status=500)

    def preprocess_numerical_data(self, df, numerical_columns):
        """Preprocess numerical columns to handle infinity and non-numeric values."""
        if not numerical_columns:
            numerical_columns = df.select_dtypes(include=['int64', 'float64']).columns
        
        for col in numerical_columns:
            if col in df.columns:
                # Convert to numeric, coerce errors to NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')
                # Replace infinity values with NaN
                df[col] = df[col].replace([np.inf, -np.inf], np.nan)
        return df

    def clean_dataframe(self, df):
        """Clean the dataframe before analysis."""
        # Store original column count
        original_columns = len(df.columns)
        
        # Replace infinite values with NaN
        df = df.replace([np.inf, -np.inf], np.nan)
        
        # Replace common missing value indicators
        missing_values = [
            'NA', 'N/A', 'NAN', 'NULL', 'NONE', '', 
            '#N/A', '#N/A N/A', '#NA', '-1.#IND', 
            '-1.#QNAN', '-NaN', '-nan', '1.#IND',
            '1.#QNAN', '<NA>', 'NIL', 'N.D.', 'N.D',
            'NOT AVAILABLE', 'NOT APPLICABLE', 'MISSING',
            'UNKNOWN', '?', '..', 'VALUE ERROR', 'value error', 'VALUEERROR', 'valueerror'
        ]
        normalized_missing = set(v.replace(' ', '').upper() for v in missing_values)
        # For numeric columns, replace error strings with 0
        for col in df.columns:
            if df[col].dtype in ['int64', 'float64'] or pd.api.types.is_numeric_dtype(df[col]):
                df[col] = df[col].apply(
                    lambda x: 'NA' if isinstance(x, str) and x.strip().replace(' ', '').upper() in normalized_missing else x
                )
                # Do not convert to numeric, keep as 'NA' string for missing/error
            elif df[col].dtype == 'object':
                df[col] = df[col].apply(
                    lambda x: 'NA' if isinstance(x, str) and x.strip().replace(' ', '').upper() in normalized_missing else x
                )
        # Verify column count hasn't changed
        if len(df.columns) != original_columns:
            print(f"Warning: Column count changed during cleaning. Original: {original_columns}, Current: {len(df.columns)}")
        
        return df
    def get_summary_stats(self, df):
        if df is None or df.empty:
            return {
                "dataset_info": {
                    "total_rows": 0,
                    "total_columns": 0,
                    "memory_usage_mb": 0,
                    "duplicate_rows": 0
                },
                "column_types": {
                    "numerical": [],
                    "categorical": [],
                    "datetime": [],
                    "boolean": []
                }
            }

        # Get actual column count before any type filtering
        total_columns = len(df.columns)
        return {
            "dataset_info": {
                "total_rows": len(df),
                "total_columns": total_columns,
                "memory_usage_mb": round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
                "duplicate_rows": int(df.duplicated().sum())
            },
            "column_types": {
                "numerical": df.select_dtypes(include=['int64', 'float64']).columns.tolist(),
                "categorical": df.select_dtypes(include=['object']).columns.tolist(),
                "datetime": df.select_dtypes(include=['datetime64']).columns.tolist(),
                "boolean": df.select_dtypes(include=['bool']).columns.tolist()
            }
        }

    def analyze_data_quality(self, df):
        quality_metrics = {}
        for column in df.columns:
            missing_count = df[column].isnull().sum()
            total_rows = len(df)
            
            quality_metrics[column] = {
                "missing_values": {
                    "count": int(missing_count),
                    "percentage": round((missing_count / total_rows) * 100, 2)
                },
                "unique_values": int(df[column].nunique()),
                "completeness": round(((total_rows - missing_count) / total_rows) * 100, 2)
            }

            # Add data type specific metrics
            if df[column].dtype in ['int64', 'float64']:
                quality_metrics[column]["zeros_count"] = int((df[column] == 0).sum())
                quality_metrics[column]["negative_values"] = int((df[column] < 0).sum())
            elif df[column].dtype == 'object':
                quality_metrics[column]["empty_strings"] = int((df[column] == '').sum())
                quality_metrics[column]["whitespace_only"] = int(df[column].str.isspace().sum())

        return quality_metrics

    def analyze_numerical_data(self, df, numerical_columns=None):
        if not numerical_columns:
            numerical_columns = df.select_dtypes(include=['int64', 'float64']).columns

        results = {}
        max_bins = 100  # Cap the number of bins to prevent memory errors
        for col in numerical_columns:
            if col in df.columns:
                # Only analyze if at least 80% of non-null values are numeric after conversion
                converted = pd.to_numeric(df[col], errors='coerce')
                num_numeric = converted.notnull().sum()
                num_total = df[col].notnull().sum()
                if num_total == 0 or num_numeric / num_total < 0.8:
                    continue  # skip columns that are mostly non-numeric
                data = converted.dropna()
                if len(data) > 0:
                    unique_vals = data.nunique()
                    bins = min(max_bins, unique_vals) if unique_vals > 1 else 1
                    # Only compute histogram if unique values are not excessive
                    if unique_vals <= 10000:
                        hist_values, hist_bins = np.histogram(data, bins=bins)
                        histogram = {
                            "bins": hist_bins.tolist(),
                            "counts": hist_values.tolist()
                        }
                    else:
                        histogram = None
                    results[col] = {
                        "mean": float(data.mean()),
                        "median": float(data.median()),
                        "std": float(data.std()),
                        "variance": float(data.var()),
                        "min": float(data.min()),
                        "max": float(data.max()),
                        "skewness": float(data.skew()),
                        "kurtosis": float(data.kurtosis()),
                        "histogram": histogram
                    }
        return results

    def analyze_categorical_data(self, df, categorical_columns=None):
        if not categorical_columns:
            categorical_columns = df.select_dtypes(include=['object']).columns

        results = {}
        for col in categorical_columns:
            if col in df.columns:
                value_counts = df[col].value_counts()
                results[col] = {
                    "unique_values": int(df[col].nunique()),
                    "mode": str(df[col].mode().iloc[0]) if not df[col].mode().empty else None,
                    "frequency": {
                        "top_10_categories": {
                            str(k): int(v) for k, v in value_counts.head(10).items()
                        },
                        "distribution": {
                            str(k): float(v / len(df) * 100) for k, v in value_counts.items()
                        }
                    },
                    "entropy": float(-(value_counts / len(df) * np.log2(value_counts / len(df))).sum()),
                    "value_counts": {str(k): int(v) for k, v in value_counts.head(10).items()}
                }
        return results

    def analyze_correlations(self, df, numerical_columns=None):
        if not numerical_columns:
            numerical_columns = df.select_dtypes(include=['int64', 'float64']).columns
        
        if len(numerical_columns) > 1:
            df_clean = df[numerical_columns].replace([np.inf, -np.inf], np.nan)
            correlation_matrix = df_clean.corr()
            
            # Convert to the format expected by frontend
            correlations = {}
            for col1 in correlation_matrix.columns:
                correlations[col1] = {}
                for col2 in correlation_matrix.columns:
                    if col1 != col2:
                        correlations[col1][col2] = float(correlation_matrix.loc[col1, col2])
            return correlations
        return {}

    def analyze_temporal_data(self, df, datetime_columns=None):
        results = {}
        for col in datetime_columns:
            if col in df.columns:
                try:
                    dates = pd.to_datetime(df[col])
                    results[col] = {
                        "range": {
                            "start": dates.min().strftime('%Y-%m-%d %H:%M:%S'),
                            "end": dates.max().strftime('%Y-%m-%d %H:%M:%S'),
                            "span_days": int((dates.max() - dates.min()).days)
                        },
                        "patterns": {
                            "yearly": dates.dt.year.value_counts().to_dict(),
                            "monthly": dates.dt.month.value_counts().to_dict(),
                            "weekly": dates.dt.isocalendar().week.value_counts().to_dict(),
                            "daily": dates.dt.day.value_counts().to_dict(),
                            "weekday": dates.dt.dayofweek.value_counts().to_dict()
                        },
                        "seasonality": {
                            "quarter": dates.dt.quarter.value_counts().to_dict(),
                            "month_name": dates.dt.month_name().value_counts().to_dict()
                        }
                    }
                except Exception:
                    continue
        return results

    def get_data_quality_viz(self, df):
        missing_data = df.isnull().sum()
        return {
            "missing_values": {
                "type": "bar",
                "data": {
                    "labels": missing_data.index.tolist(),
                    "values": missing_data.values.tolist()
                },
                "layout": {
                    "title": "Missing Values by Column",
                    "xaxis": "Columns",
                    "yaxis": "Count"
                }
            }
        }

    def get_numerical_viz(self, df, numerical_columns):
        viz_data = {}
        max_bins = 100  # Cap the number of bins to prevent memory errors
        for col in numerical_columns:
            if col in df.columns:
                # Ensure data is numeric before visualization
                data = pd.to_numeric(df[col], errors='coerce').dropna()
                if len(data) > 0:
                    unique_vals = data.nunique()
                    bins = min(max_bins, unique_vals) if unique_vals > 1 else 1
                    if unique_vals <= 10000:
                        hist_values, hist_bins = np.histogram(data, bins=bins)
                        histogram = {
                            "values": data.tolist(),
                            "bins": hist_bins.tolist()
                        }
                    else:
                        histogram = None
                    viz_data[col] = {
                        "histogram": {
                            "type": "histogram",
                            "data": histogram if histogram else {},
                            "layout": {
                                "title": f"Distribution of {col}",
                                "xaxis": col,
                                "yaxis": "Frequency"
                            }
                        },
                        "box_plot": {
                            "type": "box",
                            "data": {
                                "values": data.tolist()
                            },
                            "layout": {
                                "title": f"Box Plot of {col}"
                            }
                        }
                    }
        return viz_data

    def get_categorical_viz(self, df, categorical_columns):
        viz_data = {}
        for col in categorical_columns:
            if col in df.columns:
                value_counts = df[col].value_counts()
                viz_data[col] = {
                    "pie_chart": {
                        "type": "pie",
                        "data": {
                            "labels": value_counts.index.tolist(),
                            "values": value_counts.values.tolist()
                        },
                        "layout": {
                            "title": f"Distribution of {col}"
                        }
                    },
                    "bar_chart": {
                        "type": "bar",
                        "data": {
                            "labels": value_counts.index.tolist(),
                            "values": value_counts.values.tolist()
                        },
                        "layout": {
                            "title": f"Frequency of {col}",
                            "xaxis": col,
                            "yaxis": "Count"
                        }
                    }
                }
        return viz_data

    def get_correlation_viz(self, df, numerical_columns):
        if not numerical_columns or len(numerical_columns) < 2:
            return {}

        correlation_matrix = df[numerical_columns].corr()
        return {
            "heatmap": {
                "type": "heatmap",
                "data": {
                    "x": correlation_matrix.columns.tolist(),
                    "y": correlation_matrix.columns.tolist(),
                    "z": correlation_matrix.values.tolist()
                },
                "layout": {
                    "title": "Correlation Matrix",
                    "xaxis": "Variables",
                    "yaxis": "Variables"
                }
            }
        }

    def get_temporal_viz(self, df, datetime_columns):
        viz_data = {}
        for col in datetime_columns:
            if col in df.columns:
                try:
                    dates = pd.to_datetime(df[col])
                    time_series = dates.value_counts().sort_index()
                    viz_data[col] = {
                        "time_series": {
                            "type": "line",
                            "data": {
                                "x": time_series.index.strftime('%Y-%m-%d').tolist(),
                                "y": time_series.values.tolist()
                            },
                            "layout": {
                                "title": f"Time Series Analysis of {col}",
                                "xaxis": "Date",
                                "yaxis": "Count"
                            }
                        }
                    }
                except Exception:
                    continue
        return viz_data

    def analyze_target_variable(self, df, target_column):
        if target_column not in df.columns:
            return {"error": "Target column not found in dataset"}

        target_data = df[target_column]
        result = {
            "analysis": {},
            "visualizations": {}
        }

        if pd.api.types.is_numeric_dtype(target_data):
            result["analysis"] = self.analyze_numerical_data(df[[target_column]])[target_column]
            result["visualizations"] = self.get_numerical_viz(df[[target_column]], [target_column])
        else:
            result["analysis"] = self.analyze_categorical_data(df[[target_column]])[target_column]
            result["visualizations"] = self.get_categorical_viz(df[[target_column]], [target_column])

        return result
    def post(self, request):
        try:
            # Extract payload data
            file_type = request.data.get('file_type')
            file_name = request.data.get('file_name')
            project_id = request.data.get('project_id')
            sheet_name = request.data.get('sheet_name')
            analysis_types = request.data.get('analysis_types', ['all'])
            target_column = request.data.get('target_column')
            categorical_columns = request.data.get('categorical_columns', [])
            numerical_columns = request.data.get('numerical_columns', [])
            datetime_columns = request.data.get('datetime_columns', [])

            # Clean up file_name
            file_name = os.path.basename(file_name)

            if not all([file_type, file_name, project_id, sheet_name]):
                return Response({"error": "Missing required fields"}, status=400)

            # Get project and construct file path
            try:
                project = Projects.objects.get(id=project_id)
                file_path = os.path.join(
                    settings.MEDIA_ROOT,
                    f"user_{project.user.id}/project_{project.id}/{file_type}/{file_name}/{sheet_name}"
                )
                file_path = os.path.normpath(file_path)

                if not os.path.exists(file_path):
                    return Response({"error": "File not found"}, status=404)

                # Read the file
                df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)

                # Modified EDA results structure
                eda_results = {
                    "basic_info": {
                        "total_rows": len(df),
                        "total_columns": len(df.columns),
                        "memory_usage": df.memory_usage(deep=True).sum(),
                        "duplicate_rows": df.duplicated().sum(),
                        "column_types": {}
                    },
                    "missing_values": self.analyze_missing_values(df),
                    "numerical_stats": self.analyze_numerical_data(df, numerical_columns),
                    "categorical_stats": self.analyze_categorical_data(df, categorical_columns),
                    "correlation": self.analyze_correlations(df, numerical_columns)
                }
                for column in df.columns:
                    if pd.api.types.is_numeric_dtype(df[column]):
                        eda_results["basic_info"]["column_types"][column] = "numerical"
                    elif pd.api.types.is_datetime64_any_dtype(df[column]):
                        eda_results["basic_info"]["column_types"][column] = "datetime"
                    else:
                        try:
                            pd.to_datetime(df[column])
                            eda_results["basic_info"]["column_types"][column] = "datetime"
                        except:
                            eda_results["basic_info"]["column_types"][column] = "categorical"

                eda_results = make_json_safe(eda_results)
                return Response({
                    "message": "EDA completed successfully",
                    "eda_results": eda_results
                }, status=200)

            except Projects.DoesNotExist:
                return Response({"error": "Project not found"}, status=404)

        except Exception as e:
            return Response({"error": f"Error during EDA: {str(e)}"}, status=500)

    def analyze_missing_values(self, df):
        missing_info = df.isnull().sum()
        return {
            column: {
                "missing_count": int(count),
                "missing_percentage": float(count / len(df) * 100)
            }
            for column, count in missing_info.items() if count > 0
        }

    def analyze_numerical_columns(self, df, numerical_columns=None):
        if not numerical_columns:
            numerical_columns = df.select_dtypes(include=['int64', 'float64']).columns

        results = {}
        for col in numerical_columns:
            if col in df.columns:
                data = df[col].dropna()
                data = data.replace([np.inf, -np.inf], np.nan)
                try:
                    results[col] = {
                        "mean": self.safe_float(data.mean()),
                        "median": self.safe_float(data.median()),
                        "std": self.safe_float(data.std()),
                        "min": self.safe_float(data.min()),
                        "max": self.safe_float(data.max()),
                        "quartiles": {
                            "25": self.safe_float(data.quantile(0.25)),
                            "50": self.safe_float(data.quantile(0.50)),
                            "75": self.safe_float(data.quantile(0.75))
                        },
                        "skewness": self.safe_float(data.skew()),
                        "kurtosis": self.safe_float(data.kurtosis())
                    }
                except Exception:
                    results[col] = {
                        "error": "Could not compute statistics due to invalid values"
                    }
        return results
    
    def safe_float(self, value):
    # Early return for obvious non-convertible cases
        if value is None or pd.isna(value) or np.isinf(value):
            return None
        
        # Handle string values
        if isinstance(value, str):
            value = value.strip().upper()
            
            # List of strings that should be considered as missing/NaN
            missing_strings = [
                'NA', 'N/A', 'NAN', 'NULL', 'NONE', '',
                '#N/A', '#N/A N/A', '#NA', '-1.#IND',
                '-1.#QNAN', '-NaN', '-nan', '1.#IND',
                '1.#QNAN', '<NA>', 'NIL', 'N.D.', 'N.D',
                'NOT AVAILABLE', 'NOT APPLICABLE', 'MISSING',
                'UNKNOWN', '?', '..', 'VALUE ERROR'  # Added 'VALUE ERROR'
            ]
            
            if value in missing_strings:
                return None
            
            # Handle percentage values (e.g., "42%")
            if value.endswith('%'):
                try:
                    return float(value[:-1]) / 100
                except (ValueError, TypeError):
                    return None
        
        # Handle numeric types that don't need conversion
        if isinstance(value, (int, float, np.number)):
            # Check for infinity or NaN
            if np.isinf(value) or np.isnan(value):
                return None
            return float(value)
        
        # Final conversion attempt
        try:
            float_val = float(value)
            # Additional checks for extreme values
            if abs(float_val) > 1e150 or np.isnan(float_val) or np.isinf(float_val):
                return None
            return float_val
        except (ValueError, TypeError, OverflowError):
            return None

    def analyze_categorical_columns(self, df, categorical_columns=None):
        if not categorical_columns:
            categorical_columns = df.select_dtypes(include=['object']).columns

        results = {}
        for col in categorical_columns:
            if col in df.columns:
                value_counts = df[col].value_counts()
                results[col] = {
                    "unique_values": int(df[col].nunique()),
                    "top_categories": {
                        str(k): int(v) for k, v in value_counts.head(10).items()
                    },
                    "category_distribution": {
                        str(k): float(v / len(df) * 100) for k, v in value_counts.items()
                    }
                }
        return results

    def analyze_correlations(self, df, numerical_columns=None):
        if not numerical_columns:
            numerical_columns = df.select_dtypes(include=['int64', 'float64']).columns
        
        if len(numerical_columns) > 1:
            # Replace infinite values with NaN
            df_clean = df[numerical_columns].replace([np.inf, -np.inf], np.nan)
            correlation_matrix = df_clean.corr()
            return {
                col1: {
                    col2: self.safe_float(correlation_matrix.loc[col1, col2])
                    for col2 in correlation_matrix.columns
                    if col1 != col2
                }
                for col1 in correlation_matrix.columns
            }
        return {}

    def analyze_datetime_columns(self, df, datetime_columns=None):
        results = {}
        for col in datetime_columns:
            if col in df.columns:
                try:
                    dates = pd.to_datetime(df[col])
                    results[col] = {
                        "min_date": dates.min().strftime('%Y-%m-%d %H:%M:%S'),
                        "max_date": dates.max().strftime('%Y-%m-%d %H:%M:%S'),
                        "range_days": (dates.max() - dates.min()).days,
                        "temporal_distribution": {
                            "yearly": dates.dt.year.value_counts().to_dict(),
                            "monthly": dates.dt.month.value_counts().to_dict(),
                            "weekday": dates.dt.dayofweek.value_counts().to_dict()
                        }
                    }
                except Exception:
                    continue
        return results


class PivotEDAAnalysis(APIView):
    def post(self, request):
        try:
            data = request.data.get('data')
            if not data:
                return Response({"error": "No data provided"}, status=400)
            df = pd.DataFrame(data)
            eda_results = {
                "basic_info": {
                    "total_rows": len(df),
                    "total_columns": len(df.columns),
                    "memory_usage": df.memory_usage(deep=True).sum(),
                    "duplicate_rows": df.duplicated().sum(),
                    "column_types": {
                        col: (
                            "numerical" if pd.api.types.is_numeric_dtype(df[col])
                            else "datetime" if pd.api.types.is_datetime64_any_dtype(df[col])
                            else "categorical"
                        )
                        for col in df.columns
                    }
                },
                "missing_values": self.analyze_missing_values(df),
                "numerical_stats": self.analyze_numerical_data(df),
                "categorical_stats": self.analyze_categorical_data(df),
                "correlation": self.analyze_correlations(df),
            }
            return Response({"eda_results": eda_results}, status=200)
        except Exception as e:
            return Response({"error": str(e)}, status=500)

    def safe_float(self, val):
        try:
            if val is None:
                return None
            f = float(val)
            if math.isinf(f) or math.isnan(f):
                return None
            return f
        except Exception:
            return None

    def analyze_missing_values(self, df):
        missing_info = df.isnull().sum()
        return {
            column: {
                "missing_count": int(count),
                "missing_percentage": float(count / len(df) * 100) if len(df) > 0 else 0
            }
            for column, count in missing_info.items() if count > 0
        }

    def analyze_numerical_data(self, df):
        numerical_columns = df.select_dtypes(include=['int64', 'float64']).columns
        results = {}
        for col in numerical_columns:
            if col in df.columns:
                data = df[col].dropna()
                try:
                    stats = {
                        "mean": self.safe_float(data.mean()) if not data.empty else None,
                        "median": self.safe_float(data.median()) if not data.empty else None,
                        "std": self.safe_float(data.std()) if not data.empty else None,
                        "min": self.safe_float(data.min()) if not data.empty else None,
                        "max": self.safe_float(data.max()) if not data.empty else None,
                    }
                    if not data.empty:
                        hist_counts, hist_bins = np.histogram(data, bins='auto')
                        stats["histogram"] = {
                            "bins": [self.safe_float(b) for b in hist_bins.tolist()],
                            "counts": [int(c) for c in hist_counts.tolist()]
                        }
                    results[col] = stats
                except Exception:
                    results[col] = {"error": "Could not compute statistics"}
        return results

    def analyze_categorical_data(self, df):
        categorical_columns = df.select_dtypes(include=['object']).columns
        results = {}
        for col in categorical_columns:
            if col in df.columns:
                value_counts = df[col].value_counts()
                results[col] = {
                    "unique_values": int(df[col].nunique()),
                    "top_categories": {
                        str(k): int(v) for k, v in value_counts.head(10).items()
                    }
                }
        return results

    def analyze_correlations(self, df):
        numerical_columns = df.select_dtypes(include=['int64', 'float64']).columns
        if len(numerical_columns) > 1:
            df_clean = df[numerical_columns].replace([float('inf'), float('-inf')], None)
            correlation_matrix = df_clean.corr().fillna(0)
            return {
                col1: {
                    col2: self.safe_float(correlation_matrix.loc[col1, col2])
                    for col2 in correlation_matrix.columns if col1 != col2
                }
                for col1 in correlation_matrix.columns
            }
        return {}


class DownloadEDAExcel(APIView):
    parser_classes = [JSONParser]

    def post(self, request):
        eda_results = request.data.get('eda_results')
        personalized_plots = request.data.get('personalized_plots')
        if not eda_results:
            return Response({'error': 'No EDA results provided'}, status=400)

        wb = Workbook()
        ws_summary = wb.active
        ws_summary.title = 'Summary'

        # Write basic info
        basic_info = eda_results.get('basic_info') or eda_results.get('summary', {}).get('dataset_info', {})
        if basic_info:
            ws_summary.append(['Metric', 'Value'])
            for k, v in basic_info.items():
                if isinstance(v, dict):
                    continue
                ws_summary.append([k, v])

        # Write missing values
        missing_values = eda_results.get('missing_values') or eda_results.get('data_quality')
        if missing_values:
            ws_missing = wb.create_sheet('Missing Values')
            ws_missing.append(['Column', 'Missing Count', 'Missing %'])
            for col, stats in missing_values.items():
                ws_missing.append([col, stats.get('missing_count') or stats.get('count'), stats.get('missing_percentage') or stats.get('percentage')])
            # Add bar chart for missing values
            chart = BarChart()
            chart.title = 'Missing Values by Column'
            chart.x_axis.title = 'Column'
            chart.y_axis.title = 'Missing Count'
            data = Reference(ws_missing, min_col=2, min_row=1, max_row=ws_missing.max_row, max_col=2)
            cats = Reference(ws_missing, min_col=1, min_row=2, max_row=ws_missing.max_row)
            chart.add_data(data, titles_from_data=True)
            chart.set_categories(cats)
            ws_missing.add_chart(chart, 'E2')

        # Write numerical stats
        numerical_stats = eda_results.get('numerical_stats') or eda_results.get('detailed_analysis', {}).get('numerical')
        if numerical_stats:
            ws_num = wb.create_sheet('Numerical Stats')
            ws_num.append(['Column', 'Mean', 'Median', 'Std', 'Variance', 'Min', 'Max'])
            for col, stats in numerical_stats.items():
                ws_num.append([
                    col,
                    stats.get('mean'),
                    stats.get('median'),
                    stats.get('std'),
                    stats.get('variance'),
                    stats.get('min'),
                    stats.get('max'),
                ])
            # Add histogram/bar chart for each numerical column with histogram data
            for col, stats in numerical_stats.items():
                if 'histogram' in stats:
                    hist = stats['histogram']
                    ws_hist = wb.create_sheet(f'{col} Histogram')
                    ws_hist.append(['Bin', 'Count'])
                    for bin_val, count in zip(hist['bins'], hist['counts']):
                        ws_hist.append([bin_val, count])
                    chart = BarChart()
                    chart.title = f'Histogram of {col}'
                    chart.x_axis.title = 'Bin'
                    chart.y_axis.title = 'Count'
                    data = Reference(ws_hist, min_col=2, min_row=1, max_row=ws_hist.max_row, max_col=2)
                    cats = Reference(ws_hist, min_col=1, min_row=2, max_row=ws_hist.max_row)
                    chart.add_data(data, titles_from_data=True)
                    chart.set_categories(cats)
                    ws_hist.add_chart(chart, 'E2')

        # Write categorical stats
        categorical_stats = eda_results.get('categorical_stats') or eda_results.get('detailed_analysis', {}).get('categorical')
        if categorical_stats:
            ws_cat = wb.create_sheet('Categorical Stats')
            ws_cat.append(['Column', 'Unique Values', 'Top Category', 'Top Count'])
            for col, stats in categorical_stats.items():
                top_cat = None
                top_count = None
                if 'top_categories' in stats:
                    top_items = list(stats['top_categories'].items())
                    if top_items:
                        top_cat, top_count = top_items[0]
                ws_cat.append([
                    col,
                    stats.get('unique_values'),
                    top_cat,
                    top_count
                ])
            # Pie chart for each categorical column with top_categories
            for col, stats in categorical_stats.items():
                if 'top_categories' in stats:
                    ws_pie = wb.create_sheet(f'{col} Pie')
                    ws_pie.append(['Category', 'Count'])
                    for cat, count in stats['top_categories'].items():
                        ws_pie.append([cat, count])
                    chart = PieChart()
                    chart.title = f'Pie Chart of {col}'
                    data = Reference(ws_pie, min_col=2, min_row=1, max_row=ws_pie.max_row, max_col=2)
                    labels = Reference(ws_pie, min_col=1, min_row=2, max_row=ws_pie.max_row)
                    chart.add_data(data, titles_from_data=True)
                    chart.set_categories(labels)
                    ws_pie.add_chart(chart, 'E2')

        # Write correlation matrix
        correlation = eda_results.get('correlation') or eda_results.get('detailed_analysis', {}).get('correlations')
        if correlation:
            ws_corr = wb.create_sheet('Correlations')
            cols = list(correlation.keys())
            ws_corr.append([''] + cols)
            for row in cols:
                ws_corr.append([row] + [correlation[row].get(col, '') for col in cols])
            # Add heatmap as a bar chart for the first row (Excel doesn't support heatmaps natively)
            chart = BarChart()
            chart.title = 'Correlation (First Variable)'
            chart.x_axis.title = 'Variable'
            chart.y_axis.title = 'Correlation'
            data = Reference(ws_corr, min_col=2, min_row=1, max_row=2, max_col=len(cols)+1)
            cats = Reference(ws_corr, min_col=2, min_row=1, max_row=1, max_col=len(cols)+1)
            chart.add_data(data, titles_from_data=True)
            chart.set_categories(cats)
            ws_corr.add_chart(chart, 'E10')

        # --- Add personalized plots if present ---
        if personalized_plots:
            for idx, plot in enumerate(personalized_plots):
                chart_type = plot.get('chartType', 'bar').lower()
                chart_data = plot.get('chartData', {})
                labels = chart_data.get('labels', [])
                datasets = chart_data.get('datasets', [])
                sheet_title = f'Personalized Plot {idx+1}'
                ws = wb.create_sheet(title=sheet_title[:31])
                # Write header
                ws.append(['Label'] + [ds.get('label', f'Data{i+1}') for i, ds in enumerate(datasets)])
                # Write data
                for i, label in enumerate(labels):
                    row = [label]
                    for ds in datasets:
                        data = ds.get('data', [])
                        row.append(data[i] if i < len(data) else None)
                    ws.append(row)
                # Add chart for all datasets
                if labels and datasets:
                    data_len = len(labels)
                    data_ref = Reference(ws, min_col=2, min_row=1, max_row=data_len+1, max_col=1+len(datasets))
                    cats_ref = Reference(ws, min_col=1, min_row=2, max_row=data_len+1)
                    if chart_type == 'line':
                        chart = LineChart()
                    else:
                        chart = BarChart()
                        chart.type = "col"
                        if chart_type == 'stacked':
                            chart.grouping = 'stacked'
                        elif chart_type in ['stacked100', 'stacked_100', '100stacked', '100%stacked']:
                            chart.grouping = 'percentStacked'
                    chart.title = sheet_title
                    chart.x_axis.title = 'Label'
                    chart.y_axis.title = 'Value'
                    chart.add_data(data_ref, titles_from_data=True)
                    chart.set_categories(cats_ref)
                    ws.add_chart(chart, f"E2")

        # Save to a temporary file and return as response
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            wb.save(tmp.name)
            tmp.seek(0)
            response = HttpResponse(tmp.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename="eda_report.xlsx"'
            return response


class DownloadEDAPPTX(APIView):
    parser_classes = [JSONParser]

    def post(self, request):
        eda_results = request.data.get('eda_results')
        personalized_plots = request.data.get('personalized_plots')
        if not eda_results:
            return Response({'error': 'No EDA results provided'}, status=400)

        prs = Presentation()
        title_slide_layout = prs.slide_layouts[0]
        slide = prs.slides.add_slide(title_slide_layout)
        slide.shapes.title.text = "EDA Report"
        slide.placeholders[1].text = "Generated by Sanitifi"

        # Numerical plots (histograms)
        numerical_stats = eda_results.get('numerical_stats') or eda_results.get('detailed_analysis', {}).get('numerical')
        if numerical_stats:
            for col, stats in numerical_stats.items():
                hist = stats.get('histogram')
                if hist and hist.get('bins') and hist.get('counts'):
                    slide = prs.slides.add_slide(prs.slide_layouts[5])
                    slide.shapes.title.text = f"Histogram: {col}"
                    chart_data = CategoryChartData()
                    bins = [str(b) for b in hist['bins'][:-1]]
                    chart_data.categories = bins
                    chart_data.add_series('Count', hist['counts'])
                    x, y, cx, cy = Inches(1), Inches(1.5), Inches(8), Inches(4)
                    chart = slide.shapes.add_chart(
                        XL_CHART_TYPE.COLUMN_CLUSTERED, x, y, cx, cy, chart_data
                    ).chart
                    chart.has_legend = False
                    chart.value_axis.has_major_gridlines = True
                    chart.category_axis.tick_labels.font.size = Pt(10)
                    chart.value_axis.tick_labels.font.size = Pt(10)

        # Categorical plots (pie charts)
        categorical_stats = eda_results.get('categorical_stats') or eda_results.get('detailed_analysis', {}).get('categorical')
        if categorical_stats:
            for col, stats in categorical_stats.items():
                top_cats = stats.get('top_categories') or stats.get('value_counts')
                if top_cats:
                    slide = prs.slides.add_slide(prs.slide_layouts[5])
                    slide.shapes.title.text = f"Pie Chart: {col}"
                    chart_data = CategoryChartData()
                    chart_data.categories = list(top_cats.keys())
                    chart_data.add_series('Count', list(top_cats.values()))
                    x, y, cx, cy = Inches(1), Inches(1.5), Inches(8), Inches(4)
                    chart = slide.shapes.add_chart(
                        XL_CHART_TYPE.PIE, x, y, cx, cy, chart_data
                    ).chart
                    chart.has_legend = True
                    chart.legend.font.size = Pt(10)

        # --- Add personalized plots if present ---
        if personalized_plots:
            for idx, plot in enumerate(personalized_plots):
                chart_type = plot.get('chartType', 'bar').lower()
                chart_data = plot.get('chartData', {})
                labels = chart_data.get('labels', [])
                datasets = chart_data.get('datasets', [])
                sheet_title = f'Personalized Plot {idx+1}'
                slide = prs.slides.add_slide(prs.slide_layouts[5])
                slide.shapes.title.text = sheet_title
                chart_data_obj = CategoryChartData()
                chart_data_obj.categories = labels
                for ds in datasets:
                    chart_data_obj.add_series(ds.get('label', 'Data'), ds.get('data', []))
                # Choose chart type
                if chart_type == 'line':
                    chart_type_obj = XL_CHART_TYPE.LINE
                elif chart_type == 'stacked':
                    chart_type_obj = XL_CHART_TYPE.COLUMN_STACKED
                elif chart_type in ['stacked100', 'stacked_100', '100stacked', '100%stacked']:
                    chart_type_obj = XL_CHART_TYPE.COLUMN_STACKED_100
                else:
                    chart_type_obj = XL_CHART_TYPE.COLUMN_CLUSTERED
                x_pos, y_pos, cx, cy = Inches(1), Inches(1.5), Inches(8), Inches(4)
                chart = slide.shapes.add_chart(
                    chart_type_obj, x_pos, y_pos, cx, cy, chart_data_obj
                ).chart
                chart.has_legend = True
                chart.value_axis.has_major_gridlines = True
                chart.category_axis.tick_labels.font.size = Pt(10)
                chart.value_axis.tick_labels.font.size = Pt(10)
                chart.category_axis.has_title = True
                chart.value_axis.has_title = True
                chart.category_axis.axis_title.text_frame.text = 'Label'
                chart.value_axis.axis_title.text_frame.text = 'Value'
                chart.chart_title.text_frame.text = sheet_title

        # Save to a temporary file and return as response
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pptx') as tmp:
            prs.save(tmp.name)
            tmp.seek(0)
            response = HttpResponse(tmp.read(), content_type='application/vnd.openxmlformats-officedocument.presentationml.presentation')
            response['Content-Disposition'] = 'attachment; filename="eda_report.pptx"'
            return response


class SheetCommitGraph(APIView):
    def post(self, request):
        project_id = request.data.get("project_id")
        user_id = request.data.get("user_id")
        file_name = request.data.get("file_name")
        sheet_name = request.data.get("sheet_name")
        file_type = request.data.get("file_type")

        file_name = file_name.split("\\")[-1]
        project_folder = os.path.join(settings.MEDIA_ROOT, f"user_{user_id}/project_{project_id}")
        sheet_path = os.path.join(project_folder, file_type, file_name, f"{sheet_name}")
        sheet_path = os.path.normpath(sheet_path)

        # Get commit log with parents and hashes
        result = subprocess.run(
            ["git", "log", "--pretty=format:%H|%P|%s", "--", sheet_path],
            cwd=project_folder,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            return Response({"error": "Failed to fetch commit history"}, status=500)

        commits = []
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue
            parts = line.split("|")
            commit_hash = parts[0]
            parents = parts[1].split() if parts[1] else []
            message = parts[2] if len(parts) > 2 else ""
            commits.append({
                "hash": commit_hash,
                "parents": parents,
                "message": message,
            })

        # Build children map
        hash_to_commit = {c["hash"]: c for c in commits}
        for c in commits:
            c["children"] = []
        for c in commits:
            for parent in c["parents"]:
                if parent in hash_to_commit:
                    hash_to_commit[parent]["children"].append(c["hash"])

        return Response({"commits": commits})

class AddDateColumns(APIView):
    def post(self, request):
        try:
            file_type = request.data.get('file_type')
            file_name = request.data.get('file_name')
            project_id = request.data.get('project_id')
            sheet_name = request.data.get('sheet_name')
            datetime_columns = request.data.get('datetime_columns', [])

            if not all([file_type, file_name, project_id, sheet_name, datetime_columns]):
                return Response({'error': 'Missing required fields'}, status=400)

            file_name = os.path.basename(file_name)

            try:
                project = Projects.objects.get(id=project_id)
            except Projects.DoesNotExist:
                return Response({'error': 'Project not found'}, status=404)

            file_path = os.path.join(
                settings.MEDIA_ROOT,
                f"user_{project.user.id}/project_{project.id}/{file_type}/{file_name}/{sheet_name}"
            )
            file_path = os.path.normpath(file_path)

            if not os.path.exists(file_path):
                return Response({'error': 'File not found'}, status=404)

            file_extension = os.path.splitext(sheet_name)[1].lower()
            if file_extension == '.csv':
                df = pd.read_csv(file_path)
            elif file_extension in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                return Response({'error': 'Unsupported file format'}, status=400)

            added_columns = []
            for col in datetime_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
                    date_series = pd.to_datetime(df[col], errors='coerce')

                    year_col = f'{col}_year'
                    month_col = f'{col}_month'
                    month_year_col = f'{col}_month_year'

                    if year_col not in df.columns:
                        df[year_col] = date_series.dt.year
                        added_columns.append(year_col)
                    if month_col not in df.columns:
                        df[month_col] = date_series.dt.month
                        added_columns.append(month_col)
                    if month_year_col not in df.columns:
                        df[month_year_col] = date_series.dt.strftime('%Y-%m')
                        added_columns.append(month_year_col)
                else:
                    return Response({'error': f'Column {col} not found in sheet.'}, status=400)

            # Save the updated file
            if file_extension == '.csv':
                df.to_csv(file_path, index=False)
            else:
                df.to_excel(file_path, index=False)

            # Sanitize data for JSON serialization
            import numpy as np
            def json_safe(val):
                if val is None:
                    return None
                if isinstance(val, (float, np.floating)):
                    if np.isnan(val) or np.isinf(val):
                        return None
                    return float(val)
                if isinstance(val, (int, np.integer)):
                    return int(val)
                if isinstance(val, (np.generic, np.ndarray)):
                    return val.item() if hasattr(val, "item") else str(val)
                return val

            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.astype(object).where(pd.notnull(df), None)

            # Use a robust list comprehension for sample_data
            sample_data = [[json_safe(cell) for cell in row] for row in df.head(10).values.tolist()]

            # ... after saving the updated file ...
            df = df.replace([np.inf, -np.inf], np.nan)
            df = df.astype(object).where(pd.notnull(df), None)
            def json_safe(val):
                if val is None:
                    return None
                if isinstance(val, (float, np.floating)):
                    if np.isnan(val) or np.isinf(val):
                        return None
                    return float(val)
                if isinstance(val, (int, np.integer)):
                    return int(val)
                if isinstance(val, (np.generic, np.ndarray)):
                    return val.item() if hasattr(val, "item") else str(val)
                return val
            sheet_data = {
                sheet_name: {
                    'columns': df.columns.tolist(),
                    'data': [[json_safe(cell) for cell in row] for row in df.values.tolist()]
                }
            }
            return Response({
                'message': 'Date columns added successfully',
                'added_columns': added_columns,
                'sheet_data': sheet_data
            }, status=200)
        except Exception as e:
            return Response({'error': str(e)}, status=500)


class CreateGoogleSheet(APIView):
    def post(self, request):
        file_type = request.data.get('file_type')
        file_name = request.data.get('file_name')
        project_id = request.data.get('project_id')
        sheet_name = request.data.get('sheet_name')
        if not all([file_type, file_name, project_id, sheet_name]):
            return Response({'error': 'Missing required fields'}, status=400)
        file_name = os.path.basename(file_name)
        try:
            project = Projects.objects.get(id=project_id)
        except Projects.DoesNotExist:
            return Response({'error': 'Project not found'}, status=404)
        file_path = os.path.join(
            settings.MEDIA_ROOT,
            f"user_{project.user.id}/project_{project.id}/{file_type}/{file_name}/{sheet_name}"
        )
        file_path = os.path.normpath(file_path)
        if not os.path.exists(file_path):
            return Response({'error': 'File not found'}, status=404)
        file_extension = os.path.splitext(sheet_name)[1].lower()
        try:
            if file_extension == '.csv':
                try:
                    df = pd.read_csv(file_path, encoding='utf-8')
                except UnicodeDecodeError:
                    df = pd.read_csv(file_path, encoding='latin1')
            elif file_extension in ['.xlsx', '.xls']:
                df = pd.read_excel(file_path)
            else:
                return Response({'error': 'Unsupported file format'}, status=400)
            data = [df.columns.tolist()] + df.astype(str).values.tolist()
            sheets_service, drive_service = get_gsheet_service()
            # Use a unique key for this file/sheet
            sheet_key = f"{file_type}|{file_name}|{sheet_name}"
            google_sheet_ids = project.google_sheet_ids or {}
            spreadsheet_id = google_sheet_ids.get(sheet_key)
            if spreadsheet_id:
                # Sheet already exists, update its data
                try:
                    sheets_service.spreadsheets().values().update(
                        spreadsheetId=spreadsheet_id,
                        range="Sheet1!A1",
                        valueInputOption="RAW",
                        body={"values": data}
                    ).execute()
                    # Make sure permissions are still correct (in case sheet was created before this logic)
                    try:
                        drive_service.permissions().create(
                            fileId=spreadsheet_id,
                            body={"role": "writer", "type": "anyone"},
                        ).execute()
                    except Exception:
                        pass
                except Exception as e:
                    # If update fails (e.g., sheet deleted), create a new one
                    spreadsheet_id = None
            if not spreadsheet_id:
                # Create new Google Sheet, upload data, and store mapping
                spreadsheet = sheets_service.spreadsheets().create(
                    body={"properties": {"title": f"EditData_{sheet_name}"}},
                    fields="spreadsheetId"
                ).execute()
                spreadsheet_id = spreadsheet.get('spreadsheetId')
                sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range="Sheet1!A1",
                    valueInputOption="RAW",
                    body={"values": data}
                ).execute()
                drive_service.permissions().create(
                    fileId=spreadsheet_id,
                    body={"role": "writer", "type": "anyone"},
                ).execute()
                # Save mapping
                google_sheet_ids[sheet_key] = spreadsheet_id
                project.google_sheet_ids = google_sheet_ids
                project.save()
            sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"
            return Response({"sheet_url": sheet_url, "sheet_id": spreadsheet_id}, status=200)
        except Exception as e:
            return Response({'error': f'Google Sheets error: {str(e)}'}, status=500)

class UpdateFromGoogleSheet(APIView):
    def post(self, request):
        file_type = request.data.get('file_type')
        file_name = request.data.get('file_name')
        project_id = request.data.get('project_id')
        sheet_name = request.data.get('sheet_name')
        google_sheet_id = request.data.get('google_sheet_id')
        if not all([file_type, file_name, project_id, sheet_name, google_sheet_id]):
            return Response({'error': 'Missing required fields'}, status=400)
        file_name = os.path.basename(file_name)
        try:
            project = Projects.objects.get(id=project_id)
        except Projects.DoesNotExist:
            return Response({'error': 'Project not found'}, status=404)
        file_path = os.path.join(
            settings.MEDIA_ROOT,
            f"user_{project.user.id}/project_{project.id}/{file_type}/{file_name}/{sheet_name}"
        )
        file_path = os.path.normpath(file_path)
        file_extension = os.path.splitext(sheet_name)[1].lower()
        try:
            sheets_service, _ = get_gsheet_service()
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=google_sheet_id,
                range="Sheet1"
            ).execute()
            values = result.get('values', [])
            if not values or len(values) < 2:
                return Response({'error': 'No data found in Google Sheet'}, status=400)
            df = pd.DataFrame(values[1:], columns=values[0])
            # Save to backend file (csv or xlsx)
            if file_extension == '.csv':
                try:
                    df.to_csv(file_path, index=False, encoding='utf-8')
                except Exception as e:
                    return Response({'error': f'Failed to write CSV: {str(e)}'}, status=500)
            elif file_extension in ['.xlsx', '.xls']:
                try:
                    df.to_excel(file_path, index=False, engine='openpyxl')
                except Exception as e:
                    return Response({'error': f'Failed to write Excel: {str(e)}'}, status=500)
            else:
                return Response({'error': 'Unsupported file format'}, status=400)
            # Read the file again to ensure it's saved and to return the latest data
            try:
                if file_extension == '.csv':
                    updated_df = pd.read_csv(file_path, encoding='utf-8')
                else:
                    updated_df = pd.read_excel(file_path, engine='openpyxl')
            except Exception as e:
                return Response({'error': f'Failed to read updated file: {str(e)}'}, status=500)
            # Sanitize data for JSON
            import numpy as np
            def json_safe(val):
                if val is None:
                    return None
                if isinstance(val, (float, np.floating)):
                    if np.isnan(val) or np.isinf(val):
                        return None
                    return float(val)
                if isinstance(val, (int, np.integer)):
                    return int(val)
                if isinstance(val, (np.generic, np.ndarray)):
                    return val.item() if hasattr(val, "item") else str(val)
                return val
            updated_df = updated_df.replace([np.inf, -np.inf], np.nan)
            updated_df = updated_df.astype(object).where(pd.notnull(updated_df), None)
            safe_data = [[json_safe(cell) for cell in row] for row in updated_df.values.tolist()]
            return Response({
                'message': 'Data updated from Google Sheet successfully',
                'columns': updated_df.columns.tolist(),
                'data': safe_data
            }, status=200)
        except Exception as e:
            return Response({'error': f'Google Sheets error: {str(e)}'}, status=500)


class DeleteFile(APIView):
    def delete(self, request):
        project_id = request.data.get('project_id')
        file_type = request.data.get('file_type')
        file_name = request.data.get('file_name')

        if not all([project_id, file_type, file_name]):
            return Response({'error': 'Missing required fields'}, status=400)

        try:
            project = Projects.objects.get(id=project_id)
        except Projects.DoesNotExist:
            return Response({'error': 'Project not found'}, status=404)

        file_path = os.path.join(
            settings.MEDIA_ROOT,
            f"user_{project.user.id}/project_{project.id}/{file_type}/{file_name}"
        )
        file_path = os.path.normpath(file_path)

        if not os.path.exists(file_path):
            return Response({'error': 'File or folder not found'}, status=404)

        try:
            if os.path.isfile(file_path):
                os.remove(file_path)
            else:
                import shutil
                shutil.rmtree(file_path)
            return Response({'message': 'File or folder deleted successfully'}, status=200)
        except Exception as e:
            return Response({'error': str(e)}, status=500)

class TimeoutException(Exception):
    pass

def run_with_timeout(func, args=(), kwargs={}, timeout_duration=10):
    """Run a function with a timeout using threading"""
    result = []
    error = []
    
    def target():
        try:
            result.append(func(*args, **kwargs))
        except Exception as e:
            error.append(e)
    
    thread = threading.Thread(target=target)
    thread.daemon = True
    thread.start()
    thread.join(timeout_duration)
    
    if thread.is_alive():
        # Thread is still running, timeout occurred
        raise TimeoutException("Script execution timed out")
    
    if error:
        raise error[0]
    
    return result[0]

class CustomScriptRun(APIView):
    def post(self, request):
        try:
            # Validate required fields
            required_fields = ['file_type', 'file_name', 'project_id', 'sheet_name', 'script_content']
            if not all(request.data.get(field) for field in required_fields):
                return Response({'error': 'Missing required fields'}, status=400)

            file_type = request.data['file_type']
            file_name = request.data['file_name']
            project_id = request.data['project_id']
            sheet_name = request.data['sheet_name']
            script_content = request.data['script_content']
            action = request.data.get('action', 'preview')

            # Validate action parameter
            if action not in ['preview', 'save']:
                return Response({'error': 'Invalid action parameter'}, status=400)

            # Validate script content length
            if len(script_content) > 10000:  # 10KB limit
                return Response({'error': 'Script content too large (max 10KB)'}, status=400)

            # Check for dangerous patterns in script
            dangerous_patterns = [
                r'import\s+(os|sys|subprocess|shutil|glob|socket|requests|urllib|pickle|ctypes)',
                r'__import__\s*\(',
                r'eval\s*\(',
                r'exec\s*\(',
                r'open\s*\(',
                r'file\s*\(',
                r'\.to_csv\s*\(',
                r'\.to_excel\s*\(',
                r'\.to_json\s*\(',
                r'\.to_pickle\s*\(',
                r'while\s+True:',
                r'for\s+.+\s+in\s+.+:\s*while\s+True:',
            ]

            for pattern in dangerous_patterns:
                if re.search(pattern, script_content, re.IGNORECASE):
                    return Response({'error': f'Script contains potentially dangerous pattern: {pattern}'}, status=400)

            # Get project
            try:
                project = Projects.objects.get(id=project_id)
            except Projects.DoesNotExist:
                return Response({'error': 'Project not found'}, status=404)

            # Validate file path
            file_path = os.path.join(
                settings.MEDIA_ROOT,
                f"user_{project.user.id}/project_{project.id}/{file_type}/{file_name}/{sheet_name}"
            )
            file_path = os.path.normpath(file_path)

            # Security check to prevent directory traversal
            if not file_path.startswith(os.path.normpath(settings.MEDIA_ROOT)):
                return Response({'error': 'Invalid file path'}, status=400)

            if not os.path.exists(file_path):
                return Response({'error': 'File not found'}, status=404)

            # Read the file
            file_extension = os.path.splitext(sheet_name)[1].lower()
            try:
                # Get file size in MB
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                
                if file_extension == '.csv':
                    try:
                        df = pd.read_csv(file_path, encoding='utf-8')
                    except UnicodeDecodeError:
                        df = pd.read_csv(file_path, encoding='latin1')
                elif file_extension in ['.xlsx', '.xls']:
                    df = pd.read_excel(file_path)
                else:
                    return Response({'error': 'Unsupported file format'}, status=400)
            except Exception as e:
                return Response({'error': f'Error reading file: {str(e)}'}, status=400)

            # Create a secure execution environment
            safe_globals = {
                'pd': pd,
                'np': np,
                'plt': plt,
                'sns': sns,
                'plotly': plotly,
                'px': plotly.express,
                'go': plotly.graph_objects,
                'scipy': scipy,
                'sklearn': sklearn,
                'statsmodels': statsmodels,
                'df': df.copy(),  # Work with a copy of the dataframe
                '__builtins__': {
                    'None': None,
                    'False': False,
                    'True': True,
                    'bool': bool,
                    'int': int,
                    'float': float,
                    'str': str,
                    'list': list,
                    'dict': dict,
                    'tuple': tuple,
                    'set': set,
                    'len': len,
                    'range': range,
                    'sum': sum,
                    'min': min,
                    'max': max,
                    'sorted': sorted,
                    'enumerate': enumerate,
                    'zip': zip,
                    'isinstance': isinstance,
                    'KeyError': KeyError,
                    'ValueError': ValueError,
                    'TypeError': TypeError,
                    'IndexError': IndexError,
                    'Exception': Exception,
                },
                'print': lambda *args: None,  # Disable print
            }

            def execute_script():
                exec(script_content, safe_globals)
                return safe_globals.get('df')

            try:
                # Execute the script with timeout protection
                modified_df = run_with_timeout(execute_script, timeout_duration=10)
                
                if modified_df is None or not isinstance(modified_df, pd.DataFrame):
                    return Response({'error': 'Script must modify the dataframe variable "df"'}, status=400)

                # Validate dataframe size (prevent memory exhaustion)
                if len(modified_df) > 1000000:  # 1 million rows max
                    return Response({'error': 'Resulting dataframe too large (max 1 million rows)'}, status=400)

                # Handle preview or save action
                if action == 'preview':
                    # For preview, return complete data if file is small enough
                    if file_size_mb < 50:
                        preview_df = modified_df  # Use complete dataframe
                    else:
                        preview_df = modified_df.head(100)  # Use first 100 rows for large files
                    
                    preview_data = {
                        'columns': preview_df.columns.tolist(),
                        'data': preview_df.replace([np.inf, -np.inf], np.nan)
                                     .fillna('NA')
                                     .values.tolist(),
                        'total_rows': len(modified_df),
                        'is_complete_data': file_size_mb < 50,
                        'file_size_mb': round(file_size_mb, 2)
                    }
                    return Response({
                        'message': 'Script executed successfully (preview)',
                        'preview_data': preview_data
                    }, status=200)
                else:  # save action
                    # Save the modified dataframe
                    try:
                        if file_extension == '.csv':
                            modified_df.to_csv(file_path, index=False, encoding='utf-8')
                        else:
                            modified_df.to_excel(file_path, index=False, engine='openpyxl')

                        # Commit changes to git
                        project_folder = os.path.join(settings.MEDIA_ROOT, f"user_{project.user.id}/project_{project_id}")
                        
                        try:
                            # Initialize git if it doesn't exist
                            if not os.path.exists(os.path.join(project_folder, ".git")):
                                subprocess.run(["git", "init"], cwd=project_folder, check=True)
                                subprocess.run(["git", "config", "user.name", project.user.name], cwd=project_folder, check=True)
                                subprocess.run(["git", "config", "user.email", project.user.email], cwd=project_folder, check=True)

                            # Check if there are any changes to commit
                            status_result = subprocess.run(
                                ["git", "status", "--porcelain", file_path], 
                                cwd=project_folder, 
                                capture_output=True, 
                                text=True, 
                                check=True
                            )
                            
                            if status_result.stdout.strip():
                                # Only commit if there are changes
                                commit_msg = f"Custom script execution on {sheet_name}"
                                subprocess.run(["git", "add", file_path], cwd=project_folder, check=True)
                                subprocess.run(["git", "commit", "-m", commit_msg], cwd=project_folder, check=True)
                            else:
                                print(f"No changes detected in {file_path}")
                        except subprocess.CalledProcessError as e:
                            error_msg = f"Git operation failed: {e.stderr.decode() if e.stderr else str(e)}"
                            print(error_msg)  # Log the error
                            # Continue with the response even if git commit fails
                            print("Continuing without git commit")

                        # Prepare sheet data in the same format as cleaning/melting APIs
                        modified_df = modified_df.replace([np.inf, -np.inf], np.nan)
                        modified_df = modified_df.fillna("NA")
                        
                        sheet_data = {
                            sheet_name: {
                                'columns': modified_df.columns.tolist(),
                                'data': modified_df.values.tolist()
                            }
                        }

                        return Response({
                            'message': 'Script executed and changes saved successfully',
                            'sheet_data': sheet_data
                        }, status=200)
                    except Exception as e:
                        return Response({'error': f'Error saving file: {str(e)}'}, status=400)

            except TimeoutException:
                return Response({'error': 'Script execution timed out (max 10 seconds)'}, status=400)
            except MemoryError:
                return Response({'error': 'Script exceeded memory limits'}, status=400)
            except Exception as e:
                return Response({'error': f'Script execution error: {str(e)}'}, status=400)

        except Exception as e:
            return Response({'error': f'Server error: {str(e)}'}, status=500)


class SaveScript(APIView):
    def post(self, request):
        try:
            # Validate required fields
            required_fields = ['title', 'script_content', 'user_id']
            if not all(request.data.get(field) for field in required_fields):
                return Response({'error': 'Missing required fields'}, status=400)

            # Get user from user_id in payload
            try:
                user = User.objects.get(id=request.data['user_id'])
            except User.DoesNotExist:
                return Response({'error': 'User not found'}, status=404)

            # Create new script
            script = SavedScript(
                user=user,
                title=request.data['title'],
                script_content=request.data['script_content'],
                description=request.data.get('description', '')
            )
            script.save()

            return Response({
                'message': 'Script saved successfully',
                'script_id': script.id,
                'title': script.title,
                'created_at': script.created_at,
                'updated_at': script.updated_at
            }, status=201)

        except Exception as e:
            return Response({'error': f'Error saving script: {str(e)}'}, status=400)

class FetchScripts(APIView):
    def post(self, request):  # Changed to POST to accept user_id in payload
        try:
            # Validate user_id in payload
            if not request.data.get('user_id'):
                return Response({'error': 'Missing user_id'}, status=400)

            try:
                user = User.objects.get(id=request.data['user_id'])
            except User.DoesNotExist:
                return Response({'error': 'User not found'}, status=404)

            # Get all scripts for the user
            scripts = SavedScript.objects.filter(user=user)
            
            scripts_list = [{
                'id': script.id,
                'title': script.title,
                'script_content': script.script_content,
                'description': script.description,
                'created_at': script.created_at,
                'updated_at': script.updated_at,
            } for script in scripts]

            return Response({
                'scripts': scripts_list
            }, status=200)

        except Exception as e:
            return Response({'error': f'Error fetching scripts: {str(e)}'}, status=400)

class ConcatenateSheets(APIView):
    def post(self, request):
        try:
            # Extract data from request
            project_id = request.data.get('project_id')
            file_name = request.data.get('file_name')
            file_type = request.data.get('file_type')
            sheet_names = request.data.get('sheet_names')  # List of sheet names to concatenate
            
            if not all([project_id, file_name, file_type, sheet_names]):
                return Response({'error': 'Missing required fields'}, status=400)

            file_name = os.path.basename(file_name)

            # Generate new sheet name from concatenated sheet names
            # Remove file extensions and join with '+'
            base_names = [os.path.splitext(name)[0] for name in sheet_names]
            new_sheet_name = '+'.join(base_names) + '.csv'

            # Get project
            try:
                project = Projects.objects.get(id=project_id)
            except Projects.DoesNotExist:
                return Response({'error': 'Project not found'}, status=404)

            # Construct base folder path
            base_folder = os.path.join(settings.MEDIA_ROOT, f'user_{project.user.id}/project_{project.id}/{file_type}')
            file_folder = os.path.join(base_folder, file_name)

            # Read and concatenate all sheets
            dfs = []
            for sheet_name in sheet_names:
                sheet_path = os.path.join(file_folder, sheet_name)
                sheet_path = os.path.normpath(sheet_path)
                
                if not os.path.exists(sheet_path):
                    return Response({'error': f'Sheet not found: {sheet_name}'}, status=404)

                # Read the sheet based on file extension
                file_extension = os.path.splitext(sheet_name)[1].lower()
                if file_extension == '.csv':
                    try:
                        df = pd.read_csv(sheet_path, encoding='utf-8')
                    except UnicodeDecodeError:
                        df = pd.read_csv(sheet_path, encoding='latin1')
                elif file_extension in ['.xlsx', '.xls']:
                    df = pd.read_excel(sheet_path)
                else:
                    return Response({'error': f'Unsupported file format for sheet: {sheet_name}'}, status=400)

                # Add source sheet name as a column
                df['source_sheet'] = sheet_name
                dfs.append(df)

            # Concatenate all dataframes
            if not dfs:
                return Response({'error': 'No valid sheets to concatenate'}, status=400)

            concatenated_df = pd.concat(dfs, ignore_index=True)

            # Handle infinite and NaN values before saving
            concatenated_df = concatenated_df.replace([np.inf, -np.inf], np.nan)
            concatenated_df = concatenated_df.fillna('NA')

            # Save the concatenated dataframe as CSV
            new_sheet_path = os.path.join(file_folder, new_sheet_name)
            new_sheet_path = os.path.normpath(new_sheet_path)

            # Save as CSV
            concatenated_df.to_csv(new_sheet_path, index=False, encoding='utf-8')

            # Git commit
            self.commit_to_git(
                os.path.join(settings.MEDIA_ROOT, f'user_{project.user.id}/project_{project.id}'),
                project.user,
                project.id,
                file_type,
                file_name,
                new_sheet_name
            )

            # Convert DataFrame to JSON-safe format
            safe_df = concatenated_df.copy()
            # Convert any remaining problematic values to strings
            for col in safe_df.columns:
                safe_df[col] = safe_df[col].astype(str)

            return Response({
                'message': 'Sheets concatenated successfully',
                'new_sheet': {
                    'name': new_sheet_name,
                    'columns': safe_df.columns.tolist(),
                    'data': safe_df.values.tolist()
                }
            }, status=200)

        except Exception as e:
            return Response({'error': str(e)}, status=500)

    def commit_to_git(self, project_folder, user, project_id, file_type, file_name, sheet_name):
        try:
            if not os.path.exists(os.path.join(project_folder, ".git")):
                subprocess.run(["git", "init"], cwd=project_folder)
                subprocess.run(["git", "config", "user.name", user.name], cwd=project_folder)
                subprocess.run(["git", "config", "user.email", user.email], cwd=project_folder)

            file_path_relative = os.path.join(file_type, file_name, sheet_name).replace("\\", "/")
            subprocess.run(["git", "add", file_path_relative], cwd=project_folder)
            commit_message = f"concatenate - {user.id}/{project_id}/{file_type}/{file_name}/{sheet_name}"
            subprocess.run(["git", "commit", "-m", commit_message], cwd=project_folder)
            print(f"Git commit done for {file_path_relative}")
        except Exception as e:
            print(f"Git commit failed: {str(e)}")

class SavePlot(APIView):
    parser_classes = [JSONParser]

    def post(self, request):
        try:
            # Extract data from request
            user_id = request.data.get('user_id')
            project_id = request.data.get('project_id')
            file_type = request.data.get('file_type')
            file_name = request.data.get('file_name')
            sheet_name = request.data.get('sheet_name')
            plot_name = request.data.get('plot_name', f'Plot {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')  # Default name if not provided
            plot_config = request.data.get('plot_config')
            chart_data = request.data.get('chart_data')
            chart_options = request.data.get('chart_options')

            # Validate required fields
            if not all([user_id, project_id, file_type, file_name, sheet_name, plot_config, chart_data, chart_options]):
                return Response({
                    'error': 'Missing required fields'
                }, status=400)

            # Validate file_type
            if file_type not in ['kpi', 'media']:
                return Response({
                    'error': 'Invalid file_type. Must be either "kpi" or "media"'
                }, status=400)

            # Get user and project
            try:
                user = User.objects.get(id=user_id)
                project = Projects.objects.get(id=project_id, user=user)
            except User.DoesNotExist:
                return Response({'error': 'User not found'}, status=404)
            except Projects.DoesNotExist:
                return Response({'error': 'Project not found'}, status=404)

            # Create new saved plot
            saved_plot = SavedPlot.objects.create(
                user=user,
                project=project,
                file_type=file_type,
                file_name=file_name,
                sheet_name=sheet_name,
                plot_name=plot_name,
                plot_config=plot_config,
                chart_data=chart_data,
                chart_options=chart_options
            )

            return Response({
                'message': 'Plot saved successfully',
                'plot_id': saved_plot.id,
                'plot_name': saved_plot.plot_name,
                'created_at': saved_plot.created_at,
                'updated_at': saved_plot.updated_at
            }, status=201)  # Changed to 201 for creation

        except Exception as e:
            return Response({
                'error': f'Error saving plot: {str(e)}'
            }, status=500)

@method_decorator(csrf_exempt, name='dispatch')
class FetchPlots(APIView):
    parser_classes = [JSONParser]

    def post(self, request):
        print("FetchPlots API called with data:", request.data)  # Debug log
        try:
            # Extract data from request
            user_id = request.data.get('user_id')
            project_id = request.data.get('project_id')
            file_type = request.data.get('file_type')
            file_name = request.data.get('file_name')
            sheet_name = request.data.get('sheet_name')

            print(f"Extracted data: user_id={user_id}, project_id={project_id}, file_type={file_type}, file_name={file_name}, sheet_name={sheet_name}")  # Debug log

            # Validate required fields
            if not all([user_id, project_id, file_type, file_name, sheet_name]):
                missing_fields = []
                if not user_id: missing_fields.append('user_id')
                if not project_id: missing_fields.append('project_id')
                if not file_type: missing_fields.append('file_type')
                if not file_name: missing_fields.append('file_name')
                if not sheet_name: missing_fields.append('sheet_name')
                print(f"Missing required fields: {missing_fields}")  # Debug log
                return Response({
                    'error': f'Missing required fields: {", ".join(missing_fields)}'
                }, status=400)

            # Validate file_type
            if file_type not in ['kpi', 'media']:
                print(f"Invalid file_type: {file_type}")  # Debug log
                return Response({
                    'error': 'Invalid file_type. Must be either "kpi" or "media"'
                }, status=400)

            # Get user and project
            try:
                user = User.objects.get(id=user_id)
                project = Projects.objects.get(id=project_id, user=user)
                print(f"Found user: {user.username}, project: {project.name}")  # Debug log
            except User.DoesNotExist:
                print(f"User not found with id: {user_id}")  # Debug log
                return Response({'error': 'User not found'}, status=404)
            except Projects.DoesNotExist:
                print(f"Project not found with id: {project_id} for user: {user_id}")  # Debug log
                return Response({'error': 'Project not found'}, status=404)

            # Debug: Print all plots for this user/project combination
            all_plots = SavedPlot.objects.filter(user=user, project=project)
            print(f"Total plots for user {user_id} and project {project_id}: {all_plots.count()}")
            for plot in all_plots:
                print(f"Plot ID: {plot.id}, Name: {plot.plot_name}, File: {plot.file_name}, Sheet: {plot.sheet_name}, Updated: {plot.updated_at}")

            # Fetch saved plots with specific filters
            saved_plots = SavedPlot.objects.filter(
                user=user,
                project=project,
                file_type=file_type,
                file_name=file_name,
                sheet_name=sheet_name
            ).order_by('-updated_at')

            print(f"Found {saved_plots.count()} plots matching specific filters")
            print(f"Filter criteria: file_type={file_type}, file_name={file_name}, sheet_name={sheet_name}")
            
            # Debug: Print details of each matching plot
            for plot in saved_plots:
                print(f"Matching Plot ID: {plot.id}")
                print(f"  Plot Name: {plot.plot_name}")
                print(f"  File Type: {plot.file_type}")
                print(f"  File Name: {plot.file_name}")
                print(f"  Sheet Name: {plot.sheet_name}")
                print(f"  Updated At: {plot.updated_at}")
                print("  Plot Config:", plot.plot_config)
                print("---")

            # Format response
            plots_data = [{
                'id': plot.id,
                'plot_name': plot.plot_name,
                'file_type': plot.file_type,
                'file_name': plot.file_name,
                'sheet_name': plot.sheet_name,
                'plot_config': plot.plot_config,
                'chart_data': plot.chart_data,
                'chart_options': plot.chart_options,
                'created_at': plot.created_at,
                'updated_at': plot.updated_at
            } for plot in saved_plots]

            return Response({
                'plots': plots_data,
                'debug_info': {
                    'total_plots': all_plots.count(),
                    'matching_plots': saved_plots.count(),
                    'filter_criteria': {
                        'file_type': file_type,
                        'file_name': file_name,
                        'sheet_name': sheet_name
                    }
                }
            }, status=200)

        except Exception as e:
            print(f"Error in FetchPlots: {str(e)}")  # Debug log
            return Response({
                'error': f'Error fetching plots: {str(e)}'
            }, status=500)

 