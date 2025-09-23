

 
class PivotTableAPIView(APIView):
   
    def format_indian_number(self, num):
        """Format number in Indian numbering system."""
        if pd.isna(num):
            return None
        try:
            s = str(int(num))
            if len(s) > 3:
                last_three = s[-3:]
                rest = s[:-3]
                rest = ",".join([rest[max(i - 2, 0):i] for i in range(len(rest), 0, -2)][::-1])
                return rest + "," + last_three
            else:
                return s
        except (ValueError, TypeError):
            return str(num)
 
    def parse_mapping_key(self, key):
        """Parse mapping configuration key into column-value pairs."""
        parts = key.split(',')
        column_values = {}
       
        for part in parts:
            if '-' in part:
                column, value = part.split('-', 1)
                column_values[column.strip()] = value.strip()
       
        return column_values
 
    def get_simplified_field_name(self, agg_type, column_values, mapping_key):
        """
        Generate field name in the same order as mapping_key.
        Example: "business-d2c,npd_epd-epd,brand-me" → sum|d2c|epd|me
        """
        field_name_parts = [agg_type]
 
        # Follow sequence from the mapping_key
        for part in mapping_key.split(','):
            if '-' in part:
                _, value = part.split('-', 1)
                field_name_parts.append(value.strip())
 
        return "|".join(field_name_parts)
 
 
 
    def post(self, request):
        try:
            payload = request.data
            rows = payload.get("rows", [])
            columns = payload.get("columns", [])
            values = payload.get("values", ["value"])
            aggregator = payload.get("aggregator", "sum")
            filters = payload.get("filters", {})
            datetime_grouping = payload.get("datetime_grouping", {})
            download = payload.get("download", 0)
            chart_type = payload.get("chart_type", "column")
            mapping_configurations = payload.get("mapping_configurations", {})
 
            # --- Load CSV ---
            file_path = os.path.join(settings.BASE_DIR, "media", "Common_period_data 1.csv")
            df = pd.read_csv(file_path)
            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
 
            # --- Parse date ---
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                # Remove any rows with invalid dates
                df = df[df["date"].notna()]
 
            # --- Apply filters ---
            for col, allowed_values in filters.items():
                if col in df.columns:
                    if col == "date":
                        allowed_values = [pd.to_datetime(v, errors="coerce") for v in allowed_values]
                        # Remove any invalid dates from filter values
                        allowed_values = [v for v in allowed_values if pd.notna(v)]
                        if allowed_values:
                            df = df[df[col].isin(allowed_values)]
                    else:
                        df = df[df[col].isin(allowed_values)]
 
            # --- Date grouping ---
            if "date" in df.columns and not df.empty:
                group_type = datetime_grouping.get("date")
                if group_type:
                    group_type = group_type.lower()
                    if group_type == "weekly":
                        df["date"] = df["date"] - pd.to_timedelta(df["date"].dt.weekday, unit="D")
                        df["date"] = df["date"].dt.strftime("%G W%V")
                    elif group_type == "monthly":
                        df["date"] = df["date"].dt.strftime("%Y-%m")
                    elif group_type == "quarterly":
                        df["date"] = df["date"].dt.to_period("Q").astype(str)
                        df["date"] = df["date"].str.replace("Q", "-Q")
                    elif group_type == "yearly":
                        df["date"] = df["date"].dt.strftime("%Y")
                else:
                    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
               
                # Sort by date after grouping
                df = df.sort_values("date")
 
            # --- Aggregator mapping ---
            agg_func_map = {"sum": "sum", "mean": "mean", "min": "min", "max": "max", "count": "count"}
            agg_func = agg_func_map.get(aggregator, "sum")
 
            # --- Pivot table for overall aggregations ---
            pivot_df = pd.pivot_table(
                df,
                values=values,
                index=rows,
                columns=columns,
                aggfunc=agg_func,
                fill_value=0
            )
 
            # Flatten multi-index columns
            if isinstance(pivot_df.columns, pd.MultiIndex):
                pivot_df.columns = ["_".join([str(c) for c in col if c != ""]) for col in pivot_df.columns.values]
            else:
                pivot_df.columns = pivot_df.columns.astype(str)
            pivot_df = pivot_df.reset_index()
 
            # --- Mapping config logic ---
            records = []
           
            # Get unique combinations of all row values
            if rows and not df.empty:
                # Group by all row fields to get unique combinations
                grouped = df.groupby(rows).size().reset_index()
                row_combinations = grouped[rows].to_dict('records')
            else:
                row_combinations = [{}]
 
            for row_combo in row_combinations:
                record = row_combo.copy()  # Include all row fields in the record
               
                # Filter the dataframe based on all row values
                df_filtered_by_rows = df.copy()
                for row_field, row_value in row_combo.items():
                    df_filtered_by_rows = df_filtered_by_rows[df_filtered_by_rows[row_field] == row_value]
 
                if mapping_configurations and not df_filtered_by_rows.empty:
                    for key, config in mapping_configurations.items():
                        # Parse the mapping key to get column-value pairs
                        column_values = self.parse_mapping_key(key)
                       
                        # Filter data based on all column conditions
                        df_filtered = df_filtered_by_rows.copy()
                        for column, value in column_values.items():
                            if column in df_filtered.columns:
                                df_filtered = df_filtered[df_filtered[column] == value]
                       
                        # Skip if no data matches the filter
                        if df_filtered.empty:
                            continue
                       
                        # Apply aggregations
                        for agg_type in config.get("aggregations", ["sum"]):
                            agg_type_lower = agg_type.lower()
 
                            try:
                                if agg_type_lower == "sum":
                                    agg_value = df_filtered["value"].sum()
                                elif agg_type_lower == "mean":
                                    agg_value = df_filtered["value"].mean()
                                elif agg_type_lower == "max":
                                    agg_value = df_filtered["value"].max()
                                elif agg_type_lower == "min":
                                    agg_value = df_filtered["value"].min()
                                elif agg_type_lower == "count":
                                    agg_value = df_filtered["value"].count()
                                else:
                                    agg_value = df_filtered["value"].sum()
                            except KeyError:
                                # If "value" column doesn't exist, skip this aggregation
                                continue
 
                            if config.get("absolute", True):
                                agg_value_formatted = self.format_indian_number(agg_value)
                            else:
                                agg_value_formatted = None
 
                            # Percentage calculation if requested
                            if config.get("percentage", True) and not df_filtered_by_rows.empty:
                                try:
                                    total = df_filtered_by_rows["value"].sum()
                                    pct_val = f"{agg_value / total * 100:.2f}%" if total != 0 else "0.00%"
                                except KeyError:
                                    pct_val = None
                            else:
                                pct_val = None
 
                            # Create simplified field name in format: sum|d2c|bodylotion|epd
                            field_name = self.get_simplified_field_name(agg_type_lower, column_values, key)
 
                            record[field_name] = {
                                "absolute": agg_value_formatted,
                                "percent": pct_val
                            }
 
                else:
                    # If no mapping config, just aggregate overall values
                    if not df_filtered_by_rows.empty:
                        for val_col in values:
                            if val_col in df_filtered_by_rows.columns:
                                total_val = df_filtered_by_rows[val_col].sum()
                                record[val_col] = self.format_indian_number(total_val)
 
                records.append(record)
 
 
            if download == 1:
                export_filename = f"pivot_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                export_path = os.path.join(settings.DATA_ROOT_CHART, export_filename)
               
                # Flatten the records for Excel export
                flat_records = []
                for rec in records:
                    flat_rec = rec.copy()
                    for key, value in rec.items():
                        if isinstance(value, dict):
                            if value.get("absolute") is not None:
                                # Convert comma-formatted strings to numeric values
                                if isinstance(value["absolute"], str) and ',' in value["absolute"]:
                                    numeric_value = float(value["absolute"].replace(',', ''))
                                    flat_rec[f"{key}_absolute"] = numeric_value
                                else:
                                    flat_rec[f"{key}_absolute"] = value["absolute"]
                            if value.get("percent") is not None:
                                # Convert comma-formatted strings to numeric values
                                if isinstance(value["percent"], str) and ',' in value["percent"]:
                                    numeric_value = float(value["percent"].replace(',', ''))
                                    flat_rec[f"{key}_percent"] = numeric_value
                                else:
                                    flat_rec[f"{key}_percent"] = value["percent"]
                            del flat_rec[key]
                    flat_records.append(flat_rec)
               
                # Create DataFrame and export to Excel
                df = pd.DataFrame(flat_records)
               
                # Additional check: convert any remaining string columns with commas to numeric
                for col in df.columns:
                    if df[col].dtype == 'object':  # String/object type
                        # Check if the column contains numeric values with commas
                        if df[col].apply(lambda x: isinstance(x, str) and ',' in str(x) and str(x).replace(',', '').replace('.', '').isdigit()).any():
                            df[col] = df[col].apply(lambda x: float(str(x).replace(',', '')) if isinstance(x, str) and ',' in str(x) else x)
               
                df.to_excel(export_path, index=False)
                file_url = urljoin(settings.MAINFRONTURL, f"static/{export_filename}")
                return Response({"download_url": file_url}, status=status.HTTP_200_OK)
 
            # --- Prepare chart data ---
            # Collect ALL unique metric columns from ALL records (excluding row fields)
            series_columns_set = set()
            for record in records:
                for key in record.keys():
                    if key not in rows:
                        series_columns_set.add(key)
           
            series_columns = sorted(list(series_columns_set))
           
 
            response_data = {
                "data": records,
                "columns": series_columns,
               
            }
            return Response(response_data, status=status.HTTP_200_OK)
 
        except FileNotFoundError:
            return Response({"error": "File not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            import traceback
            return Response({"error": str(e), "traceback": traceback.format_exc()}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
 
 
 
class PivotPlotAPIView(APIView):
    def post(self, request):
        try:
            payload = request.data
            x_axis = payload.get("x_axis", [])
            y_axis = payload.get("y_axis", [])
            chart_type = payload.get("chart_type", "column")
            filter_columns = payload.get("columns", [])
            datetime_grouping = payload.get("datetime_grouping", {})
            filters_payload = payload.get("filters", {})
 
            # --- Load CSV ---
            file_path = os.path.join(settings.BASE_DIR, "media", "Common_period_data 1.csv")
            df = pd.read_csv(file_path)
            df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
 
            # --- Parse date ---
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                df = df[df["date"].notna()]
                group_type = datetime_grouping.get("date", "").lower()
                if group_type == "yearly":
                    df["date"] = df["date"].dt.strftime("%Y")
                elif group_type == "monthly":
                    df["date"] = df["date"].dt.strftime("%Y-%m")
                elif group_type == "quarterly":
                    df["date"] = df["date"].dt.to_period("Q").astype(str)
                else:
                    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
 
            # --- Apply filters from payload ---
            for col, allowed_values in filters_payload.items():
                if col in df.columns:
                    df = df[df[col].astype(str).isin([str(v) for v in allowed_values])]
 
            # --- Build all x_axis combinations ---
            from itertools import product
            unique_values = [df[col].dropna().unique().tolist() for col in x_axis]
            x_combinations = [dict(zip(x_axis, vals)) for vals in product(*unique_values)]
 
            # --- Parse y_axis and aggregate ---
            series_list = []
            any_sum = False  # track if any series uses sum
            any_mean = False
            any_min = False
            any_max = False
 
            for y in y_axis:
                parts = y.split("|")
                agg = parts[0].lower()
                filters = parts[1:]
                col_filters = dict(zip(filter_columns, filters)) if filters else {}
 
                # series key & name
                key = "_".join(filters).lower() if filters else agg
                name = " ".join([f.capitalize() for f in filters]) if filters else agg.upper()
 
                data_points = []
                for combo in x_combinations:
                    temp = df.copy()
 
                    # filter by x_axis values
                    for col, val in combo.items():
                        temp = temp[temp[col] == val]
 
                    # filter by y_axis filters (columns[])
                    for col, val in col_filters.items():
                        if col in temp.columns:
                            temp = temp[temp[col] == val]
 
                    # aggregate
                    if temp.empty:
                        val = 0
                    elif agg == "sum":
                        val = temp["value"].sum()
                        val = round(val / 1e7, 1)  # convert to crores
                        any_sum = True
                    elif agg == "mean":
                        val = temp["value"].mean()
                        val = round(val / 1e5, 1)
                        any_mean= True
                    elif agg == "max":
                        val = temp["value"].max()
                        val = round(val / 1e7, 1)
                        any_max = True
                    elif agg == "min":
                        val = temp["value"].min()
                        val = round(val / 1e5, 1)
                        any_min = True
                    elif agg == "count":
                        val = temp["value"].count()
                    else:
                        val = temp["value"].sum()
                        val = round(val / 1e7, 1)
                        any_sum = True
 
                    data_points.append({"x": combo, "value": val})
 
                series_list.append({
                    "name": name,
                    "key": key,
                    "data": data_points
                })
 
            # --- Decide Y-axis title ---
            # --- Decide Y-axis title ---
            if any_sum or any_max:
                y_title = "Value (Crores)"
            elif any_mean or any_min:
                y_title = "Value (Lakhs)"
            else:
                y_title = "Value"
 
 
            # --- Build response ---
            response_data = {
                "chart": {
                    "type": chart_type,
                    "stacking": None
                },
                "x_axis": {
                    "levels": x_axis,
                    "label": " / ".join(x_axis),
                    "order": {col: sorted(df[col].dropna().unique().astype(str).tolist()) for col in x_axis}
                },
                "y_axis": {
                    "title": y_title,
                    "number_locale": "en-IN",
                    "value_format": "0.0"
                },
                "series": series_list,
                "options": {
                    "data_labels": True,
                    "tooltip": {"value_format": "0,0.[00]"},
                    "legend": True
                },
                "meta": {
                    "filters_applied": filters_payload,
                    "generated_at": datetime.utcnow().isoformat() + "Z"
                }
            }
 
            return Response(response_data, status=status.HTTP_200_OK)
 
        except FileNotFoundError:
            return Response({"error": "File not found"}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            import traceback
            return Response({"error": str(e), "traceback": traceback.format_exc()},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)
 
#
 