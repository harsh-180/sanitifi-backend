# EDA Plot Download Feature

## Overview
This feature allows users to download all their saved EDA plots along with the data used to build those plots in an Excel file. The Excel file contains editable charts that can be modified using Microsoft Excel's "Edit Data" feature.

## Features

### 1. Complete Plot Data Export
- Downloads all saved EDA plots for a specific project
- Includes the raw data used to create each plot
- Preserves plot configuration and metadata

### 2. Editable Excel Charts
- Each plot gets its own worksheet in the Excel file
- Charts are created using Excel's native charting capabilities
- Users can modify data and charts will update automatically
- Supports various chart types: bar, line, pie, scatter

### 3. Comprehensive Metadata
- Summary sheet with overview of all plots
- Individual plot metadata including:
  - Plot name and description
  - Chart type and configuration
  - X and Y axes information
  - Aggregation methods
  - Creation and update dates
  - Tags and categories

### 4. User-Friendly Instructions
- Instructions sheet explaining how to use the Excel file
- Step-by-step guide for editing charts
- Information about the project and export details

## API Endpoint

### Download EDA Plots Excel
```
POST /api/download-eda-plots-excel/
```

#### Request Body
```json
{
  "user_id": "integer",
  "project_id": "integer",
  "plot_ids": ["integer"] // Optional: specific plot IDs to download
}
```

#### Response
- Returns an Excel file (.xlsx) as a downloadable attachment
- Filename format: `eda_plots_{project_name}_{timestamp}.xlsx`

## Frontend Integration

### 1. EDA Modal
- Download button in the header with download icon
- Downloads all plots for the current project
- Shows success/error messages via toast notifications

### 2. EDA Plot Manager
- Download button in the header
- Downloads all saved plots for the project
- Accessible from the plot management interface

### 3. EDA Format Manager
- Download button in the header
- Downloads all plots (including those created from formats)
- Consistent interface across all EDA components

## Excel File Structure

### 1. Summary Sheet
- Overview of all plots in the project
- Columns: Plot Name, Chart Type, X-Axes, Y-Axes, Created Date, Description

### 2. Individual Plot Sheets
- Each plot gets its own worksheet
- Raw data used to create the chart
- Excel chart object that can be edited
- Plot metadata and configuration details

### 3. Instructions Sheet
- How to use the Excel file
- Step-by-step guide for editing charts
- Project information and export details

## Chart Types Supported

### 1. Bar Charts
- Vertical and horizontal bar charts
- Supports multiple Y-axes
- Aggregated data display

### 2. Line Charts
- Time series and trend analysis
- Multiple data series support
- Smooth line rendering

### 3. Pie Charts
- Categorical data visualization
- Percentage and value display
- Color-coded segments

### 4. Scatter Charts
- Correlation analysis
- X-Y coordinate plotting
- Multiple data point support

## Error Handling

### 1. Missing Data
- Graceful handling of plots with missing chart data
- Creates sheets with basic plot information
- Continues processing other plots

### 2. Invalid Data
- Error messages for malformed chart data
- Fallback to basic plot information
- Detailed error logging

### 3. Access Control
- User authentication required
- Project ownership verification
- Permission-based access control

## Usage Instructions

### For Users

1. **Access the Download Feature**
   - Open the EDA modal for any project
   - Click the "Download" button in the header
   - Or use the download button in Plot/Format managers

2. **Download the Excel File**
   - The file will download automatically
   - Filename includes project name and timestamp
   - File contains all saved plots for the project

3. **Edit Charts in Excel**
   - Open the Excel file in Microsoft Excel
   - Navigate to individual plot sheets
   - Click on any chart to select it
   - Right-click and select "Edit Data" or "Select Data"
   - Modify the data range or values
   - Charts will update automatically

4. **Modify Chart Appearance**
   - Right-click on chart and select "Format Chart Area"
   - Use Chart Tools ribbon for additional options
   - Customize colors, fonts, and layout

### For Developers

1. **API Integration**
   ```javascript
   const response = await fetch('/api/download-eda-plots-excel/', {
     method: 'POST',
     headers: { 'Content-Type': 'application/json' },
     body: JSON.stringify({
       user_id: userId,
       project_id: projectId,
       plot_ids: [] // Empty for all plots
     })
   });
   
   const blob = await response.blob();
   // Handle file download
   ```

2. **Error Handling**
   ```javascript
   if (!response.ok) {
     const errorData = await response.json();
     throw new Error(errorData.error || 'Download failed');
   }
   ```

3. **File Download**
   ```javascript
   const url = window.URL.createObjectURL(blob);
   const a = document.createElement('a');
   a.href = url;
   a.download = filename;
   document.body.appendChild(a);
   a.click();
   document.body.removeChild(a);
   window.URL.revokeObjectURL(url);
   ```

## Technical Implementation

### Backend (Django)
- Uses `openpyxl` library for Excel file creation
- `pandas` for data processing and conversion
- Temporary file handling for memory efficiency
- Comprehensive error handling and logging

### Frontend (React)
- Fetch API for HTTP requests
- Blob handling for file downloads
- Toast notifications for user feedback
- Consistent UI across components

### Data Processing
- Chart data conversion from JSON to DataFrame
- Excel chart creation with proper data references
- Metadata preservation and formatting
- Sheet naming and validation

## Security Considerations

1. **Authentication**
   - User ID validation required
   - Project ownership verification
   - Session-based access control

2. **Data Privacy**
   - Only user's own plots are accessible
   - Project-level access control
   - Secure file generation and delivery

3. **Input Validation**
   - Request parameter validation
   - Plot ID verification
   - Error message sanitization

## Performance Optimization

1. **Memory Management**
   - Temporary file usage for large datasets
   - Streaming response for file downloads
   - Proper cleanup of temporary files

2. **Data Processing**
   - Efficient DataFrame operations
   - Batch processing for multiple plots
   - Optimized Excel file generation

3. **User Experience**
   - Progress indicators for large downloads
   - Non-blocking UI operations
   - Responsive error handling

## Future Enhancements

1. **Selective Downloads**
   - Choose specific plots to download
   - Filter by plot type or date range
   - Custom file naming options

2. **Advanced Chart Features**
   - Support for more chart types
   - Custom chart styling options
   - Interactive chart elements

3. **Export Formats**
   - PDF export option
   - PowerPoint integration
   - CSV data export

4. **Batch Operations**
   - Multiple project export
   - Scheduled downloads
   - Email delivery options 