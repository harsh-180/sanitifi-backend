import React, { useState, useEffect } from 'react';
import { useParams, useLocation, useNavigate } from 'react-router-dom';
import { 
  FileText, 
  BarChart2, 
  PieChart, 
  LineChart, 
  Table2, 
  ChevronDown,
  ChevronUp,
  Loader2,
  ArrowLeft,
  Merge,
  Filter,
  X
} from 'lucide-react';
import { toast } from 'react-hot-toast';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "../components/ui/select";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "../components/ui/dialog";
import { Button } from "../components/ui/button";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../components/ui/table";

const ReportPage = () => {
  const { projectId } = useParams();
  const location = useLocation();
  const navigate = useNavigate();
  const projectName = location.state?.projectName || 'Project';

  // State management
  const [loading, setLoading] = useState(true);
  const [files, setFiles] = useState([]);
  const [selectedFile, setSelectedFile] = useState(null);
  const [sheets, setSheets] = useState([]);
  const [selectedSheet, setSelectedSheet] = useState(null);
  const [sheetData, setSheetData] = useState(null);
  const [columns, setColumns] = useState([]);
  const [pivotConfig, setPivotConfig] = useState({
    rows: [],
    columns: [],
    values: [],
    filters: []
  });
  const [pivotData, setPivotData] = useState(null);
  const [chartType, setChartType] = useState('bar');
  const [mergeConfig, setMergeConfig] = useState({
    primarySheet: null,
    secondarySheet: null,
    keyColumn: null
  });
  const [showMergeDialog, setShowMergeDialog] = useState(false);

  // Fetch project files on component mount
  useEffect(() => {
    fetchProjectFiles();
  }, [projectId]);

  const fetchProjectFiles = async () => {
    try {
      setLoading(true);
      const response = await fetch(`http://127.0.0.1:8000/api/report/files/${projectId}`, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error('Failed to fetch project files');
      }

      const data = await response.json();
      setFiles(data.files);
    } catch (error) {
      console.error('Error fetching files:', error);
      toast.error('Failed to load project files');
    } finally {
      setLoading(false);
    }
  };

  const handleFileSelect = async (file) => {
    try {
      setSelectedFile(file);
      setLoading(true);
      
      const response = await fetch(`http://127.0.0.1:8000/api/report/sheets/${file.id}/?project_id=${projectId}`, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error('Failed to fetch sheets');
      }

      const data = await response.json();
      setSheets(data.sheets);
      setSelectedSheet(null);
      setSheetData(null);
      setColumns([]);
      setPivotData(null);
    } catch (error) {
      console.error('Error fetching sheets:', error);
      toast.error('Failed to load sheets');
    } finally {
      setLoading(false);
    }
  };

  const handleSheetSelect = async (sheetName) => {
    try {
      setSelectedSheet(sheetName);
      setLoading(true);
      
      const response = await fetch(`http://127.0.0.1:8000/api/report/data/${selectedFile.id}/${sheetName}`, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        },
      });

      if (!response.ok) {
        throw new Error('Failed to fetch sheet data');
      }

      const data = await response.json();
      setSheetData(data.data);
      setColumns(data.columns);
      setPivotData(null);
    } catch (error) {
      console.error('Error fetching sheet data:', error);
      toast.error('Failed to load sheet data');
    } finally {
      setLoading(false);
    }
  };

  const generatePivotTable = async () => {
    try {
      setLoading(true);
      
      const response = await fetch('http://127.0.0.1:8000/api/report/pivot', {
        method: 'POST',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          fileId: selectedFile.id,
          sheetName: selectedSheet,
          config: pivotConfig
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to generate pivot table');
      }

      const data = await response.json();
      setPivotData(data.pivotData);
    } catch (error) {
      console.error('Error generating pivot table:', error);
      toast.error('Failed to generate pivot table');
    } finally {
      setLoading(false);
    }
  };

  const handleMergeSheets = async () => {
    try {
      setLoading(true);
      
      const response = await fetch('http://127.0.0.1:8000/api/report/merge', {
        method: 'POST',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          fileId: selectedFile.id,
          primarySheet: mergeConfig.primarySheet,
          secondarySheet: mergeConfig.secondarySheet,
          keyColumn: mergeConfig.keyColumn
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to merge sheets');
      }

      const data = await response.json();
      setSheetData(data.mergedData);
      setColumns(data.columns);
      setShowMergeDialog(false);
      toast.success('Sheets merged successfully');
    } catch (error) {
      console.error('Error merging sheets:', error);
      toast.error('Failed to merge sheets');
    } finally {
      setLoading(false);
    }
  };

  const generateChart = async () => {
    try {
      setLoading(true);
      
      const response = await fetch('http://127.0.0.1:8000/api/report/chart', {
        method: 'POST',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          pivotData,
          chartType,
          config: pivotConfig
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to generate chart');
      }

      const data = await response.json();
      // Handle chart data (implementation depends on your charting library)
      console.log('Chart data:', data);
    } catch (error) {
      console.error('Error generating chart:', error);
      toast.error('Failed to generate chart');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#1c2427] text-white p-6">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-4">
          <button
            onClick={() => navigate(-1)}
            className="p-2 hover:bg-[#232b2e] rounded-full transition-colors"
          >
            <ArrowLeft size={20} />
          </button>
          <h1 className="text-2xl font-bold text-[#d6ff41]">{projectName} - Report</h1>
        </div>
      </div>

      {/* Main Content */}
      <div className="grid grid-cols-12 gap-6">
        {/* Sidebar */}
        <div className="col-span-3 bg-[#232b2e] rounded-xl p-4 border border-[#d6ff41]/20">
          <h2 className="text-lg font-semibold mb-4">Files</h2>
          <div className="space-y-2 max-h-[calc(100vh-200px)] overflow-y-auto custom-scrollbar">
            {files.map((file) => (
              <button
                key={file.id}
                onClick={() => handleFileSelect(file)}
                className={`w-full p-2 text-left rounded-lg transition-colors ${
                  selectedFile?.id === file.id
                    ? 'bg-[#d6ff41]/20 text-[#d6ff41]'
                    : 'hover:bg-[#d6ff41]/10'
                }`}
              >
                <div className="flex items-center gap-2">
                  <FileText size={16} />
                  <span className="truncate">{file.name}</span>
                </div>
              </button>
            ))}
          </div>
        </div>

        {/* Main Content Area */}
        <div className="col-span-9 space-y-6">
          {/* Sheet Selection */}
          {selectedFile && (
            <div className="bg-[#232b2e] rounded-xl p-4 border border-[#d6ff41]/20">
              <h2 className="text-lg font-semibold mb-4">Sheets</h2>
              <div className="flex flex-wrap gap-2">
                {sheets.map((sheet) => (
                  <button
                    key={sheet}
                    onClick={() => handleSheetSelect(sheet)}
                    className={`px-3 py-1.5 rounded-lg transition-colors ${
                      selectedSheet === sheet
                        ? 'bg-[#d6ff41] text-black'
                        : 'bg-[#1c2427] hover:bg-[#d6ff41]/20'
                    }`}
                  >
                    {sheet}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Data Analysis Tools */}
          {selectedSheet && (
            <>
              {/* Pivot Table Configuration */}
              <div className="bg-[#232b2e] rounded-xl p-4 border border-[#d6ff41]/20">
                <h2 className="text-lg font-semibold mb-4">Pivot Table Configuration</h2>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label>Rows</Label>
                    <Select
                      value={pivotConfig.rows}
                      onValueChange={(value) => setPivotConfig(prev => ({ ...prev, rows: value }))}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select rows" />
                      </SelectTrigger>
                      <SelectContent>
                        {columns.map((col) => (
                          <SelectItem key={col} value={col}>{col}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label>Columns</Label>
                    <Select
                      value={pivotConfig.columns}
                      onValueChange={(value) => setPivotConfig(prev => ({ ...prev, columns: value }))}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select columns" />
                      </SelectTrigger>
                      <SelectContent>
                        {columns.map((col) => (
                          <SelectItem key={col} value={col}>{col}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label>Values</Label>
                    <Select
                      value={pivotConfig.values}
                      onValueChange={(value) => setPivotConfig(prev => ({ ...prev, values: value }))}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select values" />
                      </SelectTrigger>
                      <SelectContent>
                        {columns.map((col) => (
                          <SelectItem key={col} value={col}>{col}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                  <div>
                    <Label>Filters</Label>
                    <Select
                      value={pivotConfig.filters}
                      onValueChange={(value) => setPivotConfig(prev => ({ ...prev, filters: value }))}
                    >
                      <SelectTrigger>
                        <SelectValue placeholder="Select filters" />
                      </SelectTrigger>
                      <SelectContent>
                        {columns.map((col) => (
                          <SelectItem key={col} value={col}>{col}</SelectItem>
                        ))}
                      </SelectContent>
                    </Select>
                  </div>
                </div>
                <div className="mt-4 flex gap-2">
                  <Button
                    onClick={generatePivotTable}
                    className="bg-[#d6ff41] text-black hover:bg-[#e8ff6e]"
                  >
                    Generate Pivot Table
                  </Button>
                  <Dialog open={showMergeDialog} onOpenChange={setShowMergeDialog}>
                    <DialogTrigger asChild>
                      <Button variant="outline" className="border-[#d6ff41]/20">
                        <Merge size={16} className="mr-2" />
                        Merge Sheets
                      </Button>
                    </DialogTrigger>
                    <DialogContent>
                      <DialogHeader>
                        <DialogTitle>Merge Sheets</DialogTitle>
                      </DialogHeader>
                      <div className="space-y-4">
                        <div>
                          <Label>Primary Sheet</Label>
                          <Select
                            value={mergeConfig.primarySheet}
                            onValueChange={(value) => setMergeConfig(prev => ({ ...prev, primarySheet: value }))}
                          >
                            <SelectTrigger>
                              <SelectValue placeholder="Select primary sheet" />
                            </SelectTrigger>
                            <SelectContent>
                              {sheets.map((sheet) => (
                                <SelectItem key={sheet} value={sheet}>{sheet}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div>
                          <Label>Secondary Sheet</Label>
                          <Select
                            value={mergeConfig.secondarySheet}
                            onValueChange={(value) => setMergeConfig(prev => ({ ...prev, secondarySheet: value }))}
                          >
                            <SelectTrigger>
                              <SelectValue placeholder="Select secondary sheet" />
                            </SelectTrigger>
                            <SelectContent>
                              {sheets.map((sheet) => (
                                <SelectItem key={sheet} value={sheet}>{sheet}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <div>
                          <Label>Key Column</Label>
                          <Select
                            value={mergeConfig.keyColumn}
                            onValueChange={(value) => setMergeConfig(prev => ({ ...prev, keyColumn: value }))}
                          >
                            <SelectTrigger>
                              <SelectValue placeholder="Select key column" />
                            </SelectTrigger>
                            <SelectContent>
                              {columns.map((col) => (
                                <SelectItem key={col} value={col}>{col}</SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>
                        <Button
                          onClick={handleMergeSheets}
                          className="w-full bg-[#d6ff41] text-black hover:bg-[#e8ff6e]"
                        >
                          Merge
                        </Button>
                      </div>
                    </DialogContent>
                  </Dialog>
                </div>
              </div>

              {/* Chart Configuration */}
              {pivotData && (
                <div className="bg-[#232b2e] rounded-xl p-4 border border-[#d6ff41]/20">
                  <h2 className="text-lg font-semibold mb-4">Chart Configuration</h2>
                  <div className="flex gap-4 mb-4">
                    <Button
                      onClick={() => setChartType('bar')}
                      className={`${chartType === 'bar' ? 'bg-[#d6ff41] text-black' : ''}`}
                    >
                      <BarChart2 size={16} className="mr-2" />
                      Bar Chart
                    </Button>
                    <Button
                      onClick={() => setChartType('line')}
                      className={`${chartType === 'line' ? 'bg-[#d6ff41] text-black' : ''}`}
                    >
                      <LineChart size={16} className="mr-2" />
                      Line Chart
                    </Button>
                    <Button
                      onClick={() => setChartType('pie')}
                      className={`${chartType === 'pie' ? 'bg-[#d6ff41] text-black' : ''}`}
                    >
                      <PieChart size={16} className="mr-2" />
                      Pie Chart
                    </Button>
                  </div>
                  <Button
                    onClick={generateChart}
                    className="bg-[#d6ff41] text-black hover:bg-[#e8ff6e]"
                  >
                    Generate Chart
                  </Button>
                </div>
              )}

              {/* Data Display */}
              <div className="bg-[#232b2e] rounded-xl p-4 border border-[#d6ff41]/20">
                <h2 className="text-lg font-semibold mb-4">Data Preview</h2>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        {columns.map((column) => (
                          <TableHead key={column}>{column}</TableHead>
                        ))}
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {sheetData?.slice(0, 10).map((row, i) => (
                        <TableRow key={i}>
                          {columns.map((column) => (
                            <TableCell key={column}>{row[column]}</TableCell>
                          ))}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </div>
            </>
          )}
        </div>
      </div>

      {/* Loading Overlay */}
      {loading && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm flex items-center justify-center z-50">
          <div className="bg-[#232b2e] rounded-xl p-8 shadow-xl flex flex-col items-center space-y-4 border-2 border-[#d6ff41]/40">
            <Loader2 className="w-12 h-12 text-[#d6ff41] animate-spin" />
            <p className="text-[#d6ff41] font-semibold">Loading...</p>
          </div>
        </div>
      )}
    </div>
  );
};

export default ReportPage; 