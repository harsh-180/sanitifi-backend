import React, { useState, useMemo, useCallback, useEffect } from "react";
import { X, Code2, Save, Eye, Loader, AlertCircle, Bookmark } from "lucide-react";
import { toast, Toaster } from "react-hot-toast";
import { useSheetContext } from "../../context/SheetContext";
import { useDataStageContext } from "../../context/DataStageContext";
import { motion, AnimatePresence } from "framer-motion";
import { createPortal } from "react-dom";
import { AgGridReact } from 'ag-grid-react';
import { AllCommunityModule, ModuleRegistry, themeQuartz } from 'ag-grid-community';

// Register all Community features
ModuleRegistry.registerModules([AllCommunityModule]);

const pagination = true;
const paginationPageSize = 10;
const paginationPageSizeSelector = [10, 50, 100];

const PreviewModal = ({ isOpen, onClose, previewData, onSave, isLoading }) => {
  const getColumnDefs = useCallback(() => {
    if (!previewData?.columns) return [];
    return previewData.columns.map((column) => ({
      field: column,
      headerName: column,
      sortable: true,
      filter: true,
      resizable: true,
      flex: 1,
      minWidth: 100,
    }));
  }, [previewData?.columns]);

  const getRowData = useCallback(() => {
    if (!previewData?.data) return [];
    return previewData.data.map((row, index) => {
      const rowObj = {};
      previewData.columns.forEach((col, colIndex) => {
        rowObj[col] = row[colIndex];
      });
      return rowObj;
    });
  }, [previewData?.data, previewData?.columns]);

  if (!isOpen) return null;

  return createPortal(
    <AnimatePresence>
      <motion.div
        className="fixed inset-0 z-[51] flex items-center justify-center backdrop-blur-sm bg-black/60"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
      >
        <motion.div
          className="relative w-[90%] max-w-[1200px] h-[90vh] bg-gradient-to-br from-[#1c2427] to-[#1a2328] rounded-xl shadow-2xl border border-[#3e5056]/50 flex flex-col overflow-hidden custom-scrollbar"
          initial={{ scale: 0.95, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          transition={{ type: "spring", damping: 25, stiffness: 300 }}
        >
          {/* Header */}
          <div className="flex justify-between items-center p-5 border-b border-[#2a363b]/70 bg-[#263238]/80 backdrop-blur-sm">
            <div className="flex items-center gap-4">
              <div className="bg-[#d6ff41]/20 p-2.5 rounded-lg">
                <Eye size={24} className="text-[#d6ff41]" />
              </div>
              <div>
                <h2 className="text-2xl font-bold text-white">
                  Preview Results
                </h2>
                <div className="flex items-center gap-2 mt-1">
                  <span className="text-sm text-gray-400">
                    Showing {previewData?.data?.length || 0} of {previewData?.total_rows || 0} rows
                  </span>
                </div>
              </div>
            </div>
            <div className="flex items-center gap-3">
              <motion.button
                onClick={onSave}
                disabled={isLoading}
                className="px-4 py-2 bg-[#d6ff41] text-black rounded-lg hover:bg-[#d6ff41]/80 transition-all duration-200 disabled:opacity-50 flex items-center shadow-md gap-2"
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
              >
                {isLoading ? (
                  <Loader size={16} className="animate-spin" />
                ) : (
                  <Save size={16} />
                )}
                Save & Run
              </motion.button>
              <motion.button
                onClick={onClose}
                className="text-gray-400 hover:text-white hover:bg-white/10 p-2 rounded-full transition-colors"
                whileHover={{ scale: 1.1 }}
                whileTap={{ scale: 0.95 }}
              >
                <X size={24} />
              </motion.button>
            </div>
          </div>

          {/* Grid Container */}
          <div className="flex-1 p-4 overflow-hidden flex flex-col">
            <div className="flex-1 min-h-0 bg-[#232b2e] rounded-lg overflow-hidden">
              <div className="h-full w-full ag-theme-alpine">
                <AgGridReact
                  className="custom-ag-grid"
                  rowData={getRowData()}
                  columnDefs={getColumnDefs()}
                  animateRows={true}
                  pagination={pagination}
                  paginationPageSize={paginationPageSize}
                  paginationPageSizeSelector={paginationPageSizeSelector}
                  theme={themeQuartz}
                  onGridReady={(params) => {
                    params.api.sizeColumnsToFit();
                  }}
                  onFirstDataRendered={(params) => {
                    params.api.sizeColumnsToFit();
                  }}
                  defaultColDef={{
                    sortable: true,
                    filter: true,
                    resizable: true,
                    minWidth: 100,
                    flex: 1,
                    cellStyle: { 
                      color: '#e2e8f0',
                      fontSize: '13px',
                      fontFamily: 'monospace'
                    },
                    headerStyle: {
                      backgroundColor: '#1c2427',
                      color: '#d6ff41',
                      fontSize: '12px',
                      fontWeight: '600',
                      borderBottom: '1px solid #3e5056'
                    }
                  }}
                  domLayout="normal"
                  enableCellTextSelection={true}
                  ensureDomOrder={true}
                  suppressRowClickSelection={true}
                  suppressCellFocus={true}
                  suppressColumnVirtualisation={true}
                  suppressRowVirtualisation={true}
                />
              </div>
            </div>
          </div>
        </motion.div>
      </motion.div>
    </AnimatePresence>,
    document.body
  );
};

const SaveScriptModal = ({ isOpen, onClose, onSave, script }) => {
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [isLoading, setIsLoading] = useState(false);

  const handleSave = async () => {
    if (!title.trim()) {
      toast.error("Please enter a script title");
      return;
    }

    setIsLoading(true);
    try {
      await onSave({ title, description, script });
      onClose();
      setTitle("");
      setDescription("");
    } catch (error) {
      toast.error(error.message || "Failed to save script");
    } finally {
      setIsLoading(false);
    }
  };

  if (!isOpen) return null;

  return createPortal(
    <AnimatePresence>
      <motion.div
        className="fixed inset-0 z-[52] flex items-center justify-center backdrop-blur-sm bg-black/60"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
      >
        <motion.div
          className="relative w-[500px] bg-gradient-to-br from-[#1c2427] to-[#1a2328] rounded-xl shadow-2xl border border-[#3e5056]/50"
          initial={{ scale: 0.95, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          transition={{ type: "spring", damping: 25, stiffness: 300 }}
        >
          <div className="p-5 border-b border-[#2a363b]/70">
            <h2 className="text-xl font-bold text-white">Save Script</h2>
          </div>

          <div className="p-5 space-y-4">
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1">Script Title</label>
              <input
                type="text"
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                className="w-full px-3 py-2 bg-[#2a3437] border border-[#3e5056] rounded-lg text-white focus:outline-none focus:border-[#d6ff41]"
                placeholder="Enter script title"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-300 mb-1">Description (Optional)</label>
              <textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                className="w-full px-3 py-2 bg-[#2a3437] border border-[#3e5056] rounded-lg text-white focus:outline-none focus:border-[#d6ff41] resize-none h-24"
                placeholder="Enter script description"
              />
            </div>
          </div>

          <div className="p-5 border-t border-[#2a363b]/70 flex justify-end gap-3">
            <motion.button
              onClick={onClose}
              className="px-4 py-2 text-gray-300 hover:text-white transition-colors"
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              Cancel
            </motion.button>
            <motion.button
              onClick={handleSave}
              disabled={isLoading}
              className="px-4 py-2 bg-[#d6ff41] text-black rounded-lg hover:bg-[#d6ff41]/80 transition-all duration-200 disabled:opacity-50 flex items-center gap-2"
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              {isLoading ? (
                <Loader size={16} className="animate-spin" />
              ) : (
                <Save size={16} />
              )}
              Save Script
            </motion.button>
          </div>
        </motion.div>
      </motion.div>
    </AnimatePresence>,
    document.body
  );
};

const CustomScriptModal = ({ isOpen, onClose, sheetData, setSheetData }) => {
  const [script, setScript] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [previewData, setPreviewData] = useState(null);
  const [showPreview, setShowPreview] = useState(false);
  const [error, setError] = useState(null);
  const [showSaveModal, setShowSaveModal] = useState(false);
  const [savedScripts, setSavedScripts] = useState([]);
  const [selectedScript, setSelectedScript] = useState(null);

  const { clickedProjectId, clickedFileName, clickedSheet, clickedFileType } =
    useSheetContext();

  const { dataStage } = useDataStageContext();

  const [gridApi, setGridApi] = useState(null);
  const [gridColumnApi, setGridColumnApi] = useState(null);

  const defaultColDef = useMemo(() => ({
    flex: 1,
    minWidth: 100,
    resizable: true,
    sortable: true,
    filter: true,
    cellStyle: { 
      color: '#e2e8f0',
      fontSize: '13px',
      fontFamily: 'monospace'
    },
    headerStyle: {
      backgroundColor: '#1c2427',
      color: '#d6ff41',
      fontSize: '12px',
      fontWeight: '600',
      borderBottom: '1px solid #3e5056'
    }
  }), []);

  const onGridReady = (params) => {
    setGridApi(params.api);
    setGridColumnApi(params.columnApi);
    params.api.sizeColumnsToFit();
  };

  const showToast = (type, message) => {
    toast[type](message, {
      position: "top-center",
      style: {
        zIndex: 100001,
        background: '#1e293b',
        color: '#fff',
        border: '1px solid #3e5056',
        borderRadius: '8px',
        boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)'
      },
      duration: 3000
    });
  };

  const executeScript = async (action) => {
    if (!script.trim()) {
      showToast('error', "Please enter a script to execute");
      return;
    }

    setIsLoading(true);
    setError(null);
    try {
      const response = await fetch(" http://127.0.0.1:8000/api/custom-script/", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          file_type: clickedFileType,
          file_name: clickedFileName,
          project_id: clickedProjectId,
          sheet_name: clickedSheet,
          script_content: script,
          action: action,
        }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.error || "Failed to execute script");
      }

      if (action === "preview") {
        if (data.preview_data) {
          setPreviewData(data.preview_data);
          setShowPreview(true);
          showToast('success', "Preview generated successfully");
        } else {
          throw new Error("Invalid preview data received");
        }
      } else {
        if (setSheetData && data.sheet_data) {
          const sheetKey = Object.keys(data.sheet_data)[0];
          if (!sheetKey) {
            throw new Error("No sheet data found in response");
          }

          const sheetData = data.sheet_data[sheetKey];
          if (!sheetData || !sheetData.columns || !sheetData.data) {
            throw new Error("Invalid sheet data structure in response");
          }

          const updatedSheetData = {
            columns: sheetData.columns,
            data: sheetData.data,
            column_types: sheetData.column_types || {}
          };

          setSheetData(updatedSheetData);
          showToast('success', data.message || "Script executed and changes saved successfully");
          onClose();
          setShowPreview(false);
        } else {
          throw new Error("Failed to update sheet data - missing setSheetData function or sheet data");
        }
      }
    } catch (error) {
      setError(error.message || "Failed to execute script");
      showToast('error', error.message || "Failed to execute script");
      console.error("Script execution error:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handlePreview = () => executeScript("preview");
  const handleSave = () => executeScript("save");

  const getColumnDefs = useCallback(() => {
    if (!previewData?.columns) return [];
    return previewData.columns.map((column) => ({
      field: column,
      headerName: column,
      sortable: true,
      filter: true,
      resizable: true,
      flex: 1,
      minWidth: 100,
    }));
  }, [previewData?.columns]);

  const getRowData = useCallback(() => {
    if (!previewData?.data) return [];
    return previewData.data.map((row, index) => {
      const rowObj = {};
      previewData.columns.forEach((col, colIndex) => {
        rowObj[col] = row[colIndex];
      });
      return rowObj;
    });
  }, [previewData?.data, previewData?.columns]);

  const rowClassRules = {
    "rag-red": (params) => params.data.make === "Ford",
  };

  // Add helper text component
  const ScriptGuide = () => (
    <div className="text-sm text-gray-400 space-y-2 mb-4">
      <div className="bg-[#1c2427] p-4 rounded-lg border border-[#3e5056]/50">
        <h3 className="text-[#d6ff41] font-medium mb-2">Available Imports & Variables:</h3>
        <ul className="list-disc list-inside space-y-1">
          <li><code className="text-[#d6ff41]">pd</code> - pandas library</li>
          <li><code className="text-[#d6ff41]">np</code> - numpy library</li>
          <li><code className="text-[#d6ff41]">plt</code> - matplotlib.pyplot</li>
          <li><code className="text-[#d6ff41]">sns</code> - seaborn</li>
          <li><code className="text-[#d6ff41]">plotly</code> - plotly library</li>
          <li><code className="text-[#d6ff41]">px</code> - plotly.express</li>
          <li><code className="text-[#d6ff41]">go</code> - plotly.graph_objects</li>
          <li><code className="text-[#d6ff41]">scipy</code> - scipy library</li>
          <li><code className="text-[#d6ff41]">sklearn</code> - scikit-learn</li>
          <li><code className="text-[#d6ff41]">statsmodels</code> - statsmodels library</li>
          <li><code className="text-[#d6ff41]">df</code> - your dataframe (must be modified and returned)</li>
        </ul>
      </div>
      
      <div className="bg-[#1c2427] p-4 rounded-lg border border-[#3e5056]/50">
        <h3 className="text-[#d6ff41] font-medium mb-2">Available Built-in Functions:</h3>
        <ul className="list-disc list-inside space-y-1">
          <li>Basic types: <code className="text-[#d6ff41]">int, float, str, bool, list, dict, tuple, set</code></li>
          <li>Functions: <code className="text-[#d6ff41]">len, range, sum, min, max, sorted, enumerate, zip</code></li>
        </ul>
      </div>

      <div className="bg-[#1c2427] p-4 rounded-lg border border-[#3e5056]/50">
        <h3 className="text-[#d6ff41] font-medium mb-2">Important Notes:</h3>
        <ul className="list-disc list-inside space-y-1">
          <li>Script must modify and return the <code className="text-[#d6ff41]">df</code> variable</li>
          <li>Maximum script size: 10KB</li>
          <li>Execution timeout: 10 seconds</li>
          <li>Maximum rows in result: 1 million</li>
          <li>Preview shows first 100 rows for files larger than 50MB</li>
          <li>Changes are automatically saved to the file and committed to git</li>
        </ul>
      </div>

      <div className="bg-[#1c2427] p-4 rounded-lg border border-[#3e5056]/50">
        <h3 className="text-[#d6ff41] font-medium mb-2">Restricted Operations:</h3>
        <ul className="list-disc list-inside space-y-1">
          <li>No file operations (<code className="text-[#d6ff41]">open, to_csv, to_excel, etc.</code>)</li>
          <li>No system operations (<code className="text-[#d6ff41]">os, sys, subprocess, etc.</code>)</li>
          <li>No network operations (<code className="text-[#d6ff41]">requests, urllib, etc.</code>)</li>
          <li>No infinite loops or potentially dangerous patterns</li>
        </ul>
      </div>
    </div>
  );

  const getUserId = useCallback(() => {
    return localStorage.getItem("user_id");
  }, []);

  const fetchSavedScripts = useCallback(async () => {
    const userId = getUserId();
    if (!userId) {
      toast.error("Please login to access saved scripts");
      return;
    }

    try {
      console.log('Fetching scripts for user:', userId); // Debug log
      const response = await fetch(" http://127.0.0.1:8000/api/fetch_scripts/", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({  
          user_id: userId
        }),
        mode: "cors", // Add CORS mode
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to fetch saved scripts");
      }
      const data = await response.json();
      console.log('Fetched scripts:', data); // Debug log
      setSavedScripts(data.scripts || []);
    } catch (error) {
      console.error("Error fetching saved scripts:", error);
      toast.error(error.message || "Failed to fetch saved scripts");
    }
  }, [getUserId]);

  useEffect(() => {
    if (isOpen) {
      fetchSavedScripts();
    }
  }, [isOpen, fetchSavedScripts]);

  const handleSaveScript = async (scriptData) => {
    const userId = getUserId();
    if (!userId) {
      toast.error("Please login to save scripts");
      return;
    }

    try {
      console.log('Saving script for user:', userId); // Debug log
      const response = await fetch(" http://127.0.0.1:8000/api/save_script/", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          title: scriptData.title,
          description: scriptData.description,
          script_content: scriptData.script,
          user_id: userId
        }),
        mode: "cors", // Add CORS mode
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to save script");
      }

      const data = await response.json();
      console.log('Save script response:', data); // Debug log
      toast.success("Script saved successfully");
      fetchSavedScripts(); // Refresh the list
    } catch (error) {
      console.error("Error saving script:", error);
      toast.error(error.message || "Failed to save script");
    }
  };

  const handleLoadScript = (script) => {
    setScript(script.script_content);
    setSelectedScript(script);
  };

  if (!isOpen) return null;

  return (
    <>
      {createPortal(
        <AnimatePresence>
          <motion.div
            className="fixed inset-0 z-50 flex items-center justify-center backdrop-blur-sm bg-black/60"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <motion.div
              className="relative w-[80%] max-w-[900px] h-[85vh] bg-gradient-to-br from-[#1c2427] to-[#1a2328] rounded-xl shadow-2xl border border-[#3e5056]/50 flex flex-col overflow-hidden custom-scrollbar"
              initial={{ scale: 0.95, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              transition={{ type: "spring", damping: 25, stiffness: 300 }}
            >
              {/* Header */}
              <div className="flex justify-between items-center p-5 border-b border-[#2a363b]/70 bg-gradient-to-r from-[#1c2427] to-[#263238] backdrop-blur-sm">
                <div className="flex items-center gap-4">
                  <div className="bg-gradient-to-br from-[#d6ff41]/20 to-[#d6ff41]/10 p-3 rounded-xl border border-[#d6ff41]/20 shadow-lg shadow-[#d6ff41]/5">
                    <Code2 size={24} className="text-[#d6ff41]" />
                  </div>
                  <div className="space-y-1">
                    <div className="flex items-center gap-3">
                      <h2 className="text-2xl font-bold text-white">
                        Custom Script Editor
                      </h2>
                      <div className="flex items-center gap-2">
                        <span className="px-2.5 py-1 text-xs font-medium bg-[#2a3437] text-gray-300 rounded-md border border-[#3e5056]/50">
                          {clickedFileType || "file"}
                        </span>
                        <span className="px-2.5 py-1 text-xs font-medium bg-[#d6ff41]/10 text-[#d6ff41] rounded-md border border-[#d6ff41]/20">
                          {dataStage || "raw"}
                        </span>
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      <div className="flex items-center gap-2 text-sm text-gray-400">
                        <span className="font-medium text-gray-300">{clickedFileName}</span>
                        <span className="text-gray-500">•</span>
                        <span className="text-gray-300">{clickedSheet}</span>
                      </div>
                      <div className="h-4 w-[1px] bg-[#3e5056]/50"></div>
                      <div className="text-sm text-gray-400">
                        {sheetData?.columns?.length || 0} columns • {sheetData?.data?.length || 0} rows
                      </div>
                    </div>
                  </div>
                </div>
                <div className="flex items-center gap-3">
                  <select
                    value={selectedScript?.id || ""}
                    onChange={(e) => {
                      const script = savedScripts.find(s => s.id === parseInt(e.target.value));
                      if (script) handleLoadScript(script);
                    }}
                    className="px-3 py-1.5 bg-[#2a3437] border border-[#3e5056] rounded-lg text-white text-sm focus:outline-none focus:border-[#d6ff41] hover:bg-[#323c3f] transition-colors [&>option:hover]:bg-[#d6ff41] [&>option:hover]:text-black"
                  >
                    <option value="">Load Saved Script</option>
                    {savedScripts.map((script) => (
                      <option key={script.id} value={script.id}>
                        {script.title}
                      </option>
                    ))}
                  </select>
                  <motion.button
                    onClick={() => setShowSaveModal(true)}
                    className="px-4 py-2 bg-[#2a3437] text-white rounded-lg hover:bg-[#323c3f] transition-all duration-200 flex items-center gap-2 border border-[#3e5056]"
                    whileHover={{ scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                  >
                    <Bookmark size={16} />
                    Save Script
                  </motion.button>
                  <motion.button
                    onClick={onClose}
                    className="text-gray-400 hover:text-white hover:bg-white/10 p-2 rounded-lg transition-colors border border-transparent hover:border-[#3e5056]"
                    whileHover={{ scale: 1.1 }}
                    whileTap={{ scale: 0.95 }}
                  >
                    <X size={24} />
                  </motion.button>
                </div>
              </div>

              {/* Main Content */}
              <div className="flex-1 flex gap-4 p-4 overflow-hidden">
                {/* Left Side - Instructions */}
                <div className="w-[400px] flex-shrink-0 overflow-y-auto custom-scrollbar pr-2">
                  <div className="space-y-4">
                    <div className="bg-[#1c2427] p-4 rounded-lg border border-[#3e5056]/50">
                      <h3 className="text-[#d6ff41] font-medium mb-2">Available Imports & Variables:</h3>
                      <ul className="list-disc list-inside space-y-1 text-gray-300">
                        <li><code className="text-[#d6ff41]">pd</code> - pandas library</li>
                        <li><code className="text-[#d6ff41]">np</code> - numpy library</li>
                        <li><code className="text-[#d6ff41]">plt</code> - matplotlib.pyplot</li>
                        <li><code className="text-[#d6ff41]">sns</code> - seaborn</li>
                        <li><code className="text-[#d6ff41]">plotly</code> - plotly library</li>
                        <li><code className="text-[#d6ff41]">px</code> - plotly.express</li>
                        <li><code className="text-[#d6ff41]">go</code> - plotly.graph_objects</li>
                        <li><code className="text-[#d6ff41]">scipy</code> - scipy library</li>
                        <li><code className="text-[#d6ff41]">sklearn</code> - scikit-learn</li>
                        <li><code className="text-[#d6ff41]">statsmodels</code> - statsmodels library</li>
                        <li><code className="text-[#d6ff41]">df</code> - your dataframe (must be modified and returned)</li>
                      </ul>
                    </div>

                    <div className="bg-[#1c2427] p-4 rounded-lg border border-[#3e5056]/50">
                      <h3 className="text-[#d6ff41] font-medium mb-2">Current Columns:</h3>
                      {sheetData?.columns ? (
                        <div className="space-y-1">
                          {sheetData.columns.map((column, index) => (
                            <div key={index} className="flex items-center gap-2 text-gray-300">
                              <span className="text-[#d6ff41] font-mono">{column}</span>
                              {sheetData.column_types?.[column] && (
                                <span className="text-xs text-gray-500">({sheetData.column_types[column]})</span>
                              )}
                            </div>
                          ))}
                        </div>
                      ) : (
                        <p className="text-gray-400 text-sm">No columns available</p>
                      )}
                    </div>
                    
                    <div className="bg-[#1c2427] p-4 rounded-lg border border-[#3e5056]/50">
                      <h3 className="text-[#d6ff41] font-medium mb-2">Available Built-in Functions:</h3>
                      <ul className="list-disc list-inside space-y-1 text-gray-300">
                        <li>Basic types: <code className="text-[#d6ff41]">int, float, str, bool, list, dict, tuple, set</code></li>
                        <li>Functions: <code className="text-[#d6ff41]">len, range, sum, min, max, sorted, enumerate, zip</code></li>
                      </ul>
                    </div>

                    <div className="bg-[#1c2427] p-4 rounded-lg border border-[#3e5056]/50">
                      <h3 className="text-[#d6ff41] font-medium mb-2">Important Notes:</h3>
                      <ul className="list-disc list-inside space-y-1 text-gray-300">
                        <li>Script must modify and return the <code className="text-[#d6ff41]">df</code> variable</li>
                        <li>Maximum script size: 10KB</li>
                        <li>Execution timeout: 10 seconds</li>
                        <li>Maximum rows in result: 1 million</li>
                        <li>Preview shows first 100 rows for files larger than 50MB</li>
                        <li>Changes are automatically saved to the file and committed to git</li>
                      </ul>
                    </div>

                    <div className="bg-[#1c2427] p-4 rounded-lg border border-[#3e5056]/50">
                      <h3 className="text-[#d6ff41] font-medium mb-2">Restricted Operations:</h3>
                      <ul className="list-disc list-inside space-y-1 text-gray-300">
                        <li>No file operations (<code className="text-[#d6ff41]">open, to_csv, to_excel, etc.</code>)</li>
                        <li>No system operations (<code className="text-[#d6ff41]">os, sys, subprocess, etc.</code>)</li>
                        <li>No network operations (<code className="text-[#d6ff41]">requests, urllib, etc.</code>)</li>
                        <li>No infinite loops or potentially dangerous patterns</li>
                      </ul>
                    </div>

                    <div className="bg-[#1c2427] p-4 rounded-lg border border-[#3e5056]/50">
                      <h3 className="text-[#d6ff41] font-medium mb-2">Example Script:</h3>
                      <pre className="text-sm text-gray-300 font-mono whitespace-pre-wrap">
{`# Filter rows where Value > 1000
df = df[df['Value'] > 1000]

# Create a new column
df['Value_Double'] = df['Value'] * 2

# Group by Category and calculate mean
df = df.groupby('Category')['Value'].mean().reset_index()

# Sort by Value
df = df.sort_values('Value', ascending=False)

# Remember: You must modify and return the df variable`}
                      </pre>
                    </div>
                  </div>
                </div>

                {/* Right Side - Script Editor */}
                <div className="flex-1 flex flex-col min-h-0 bg-[#2a3437] rounded-lg overflow-hidden border border-[#3e5056]/50">
                  <div className="h-full flex flex-col">
                    <div className="px-4 py-2 bg-[#1c2427] border-b border-[#3e5056]/50 flex items-center justify-between">
                      <span className="text-sm font-medium text-[#d6ff41]">
                        Script Editor
                      </span>
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-gray-400">Python</span>
                        <div className="w-2 h-2 rounded-full bg-[#d6ff41]"></div>
                      </div>
                    </div>
                    <textarea
                      value={script}
                      onChange={(e) => setScript(e.target.value)}
                      placeholder="Write your Python script here..."
                      className="flex-1 w-full p-4 bg-[#2a3437] text-white font-mono text-sm resize-none focus:outline-none"
                    />
                  </div>
                </div>
              </div>

              {/* Error Message */}
              {error && (
                <motion.div
                  className="mx-4 mb-4 p-3 bg-red-500/10 border border-red-500/20 rounded-lg flex items-start gap-3"
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                >
                  <AlertCircle
                    size={18}
                    className="text-red-400 mt-0.5 flex-shrink-0"
                  />
                  <div className="flex flex-col gap-1">
                    <span className="text-sm font-medium text-red-400">Error</span>
                    <span className="text-sm text-red-300">{error}</span>
                  </div>
                </motion.div>
              )}

              {/* Footer */}
              <div className="p-4 border-t border-[#2a363b]/70 bg-[#263238]/80 backdrop-blur-sm flex justify-between items-center">
                <div className="flex items-center gap-2 text-sm text-gray-400">
                  <span className="px-2 py-1 bg-[#2a3437] rounded-md font-mono">
                    df
                  </span>
                  <span>Available in script</span>
                </div>
                <div className="flex space-x-3">
                  <motion.button
                    onClick={handlePreview}
                    disabled={isLoading}
                    className="px-4 py-2 bg-[#2a3437] text-white rounded-lg hover:bg-[#3a4447] transition-all duration-200 disabled:opacity-50 flex items-center shadow-md gap-2"
                    whileHover={{ scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                  >
                    {isLoading ? (
                      <Loader size={16} className="animate-spin" />
                    ) : (
                      <Eye size={16} />
                    )}
                    Preview
                  </motion.button>
                </div>
              </div>
            </motion.div>
          </motion.div>
        </AnimatePresence>,
        document.body
      )}

      {showPreview && createPortal(
        <PreviewModal
          isOpen={showPreview}
          onClose={() => setShowPreview(false)}
          previewData={previewData}
          onSave={handleSave}
          isLoading={isLoading}
        />,
        document.body
      )}

      <SaveScriptModal
        isOpen={showSaveModal}
        onClose={() => setShowSaveModal(false)}
        onSave={handleSaveScript}
        script={script}
      />

      <Toaster
        position="top-center"
        containerStyle={{
          position: 'fixed',
          top: '20px',
          left: 0,
          right: 0,
          zIndex: 100,
        }}
        toastOptions={{
          style: {
            background: '#1e293b',
            color: '#fff',
            border: '1px solid #3e5056',
            borderRadius: '8px',
            boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)'
          },
        }}
      />
    </>
  );
};

export default CustomScriptModal;