import React, { useState, useRef } from 'react';
import { IconAlertTriangle, IconFileText } from '@tabler/icons-react';
import { useContractAnalysis } from '../hooks/useContractAnalysis';

// Sub-components
import { ContractUploader } from '../components/contracts/ContractUploader';
import { ContractFileCard } from '../components/contracts/ContractFileCard';
import { ContractAnalysisResult } from '../components/contracts/ContractAnalysisResult';

const Contracts = () => {
    const {
        file,
        analysis,
        loading,
        error,
        progress,
        handleFile,
        removeFile,
        analyzeContract
    } = useContractAnalysis();
    
    // UI Local State for Drag & Drop
    const [dragActive, setDragActive] = useState(false);
    const fileInputRef = useRef(null);

    const handleDrag = (e) => {
        e.preventDefault();
        e.stopPropagation();
        if (e.type === 'dragenter' || e.type === 'dragover') {
            setDragActive(true);
        } else if (e.type === 'dragleave') {
            setDragActive(false);
        }
    };

    const handleDrop = (e) => {
        e.preventDefault();
        e.stopPropagation();
        setDragActive(false);
        
        if (e.dataTransfer.files && e.dataTransfer.files[0]) {
            handleFile(e.dataTransfer.files[0]);
        }
    };

    const handleChange = (e) => {
        e.preventDefault();
        if (e.target.files && e.target.files[0]) {
            handleFile(e.target.files[0]);
        }
    };

    const handleRemove = () => {
        removeFile();
        if (fileInputRef.current) {
            fileInputRef.current.value = '';
        }
    };

    return (
        <div className="flex-1 flex flex-col bg-[var(--background)] overflow-hidden relative h-full">
            <div className="flex-1 overflow-y-auto w-full custom-scrollbar pb-24 px-8 lg:px-12 pt-8">
                <div className="max-w-4xl mx-auto space-y-10">
                    <div className="mb-8">
                        <h1 className="text-[18px] font-semibold text-[var(--foreground)] flex items-center gap-2 tracking-tight">
                            Contract Analysis <span className="text-[10px] font-bold tracking-wider text-[#d4af37] bg-[var(--navbar-bg)] px-[5px] py-[1px] rounded uppercase align-middle relative -top-0.5 shadow-sm border border-[#d4af37]/30">AI</span>
                        </h1>
                        <p className="text-[13px] text-[var(--text-soft)] mt-1 font-medium">Upload legal contracts to instantly identify risks, loopholes, and obligations.</p>
                    </div>

                    {error && (
                        <div className="mb-6 p-4 bg-rose-50 border border-rose-100 rounded-xl flex items-start gap-3 text-rose-700 shadow-sm animate-in fade-in slide-in-from-top-2">
                            <IconAlertTriangle className="shrink-0 mt-0.5 text-rose-500" size={18} />
                            <div>
                                <p className="font-bold text-[13px]">Upload Exception</p>
                                <p className="text-[13px] opacity-90 mt-0.5">{error}</p>
                            </div>
                        </div>
                    )}

                    {!file ? (
                        <ContractUploader 
                            dragActive={dragActive}
                            handleDrag={handleDrag}
                            handleDrop={handleDrop}
                            handleChange={handleChange}
                            fileInputRef={fileInputRef}
                        />
                    ) : (
                        <ContractFileCard 
                            file={file}
                            loading={loading}
                            progress={progress}
                            analysis={analysis}
                            handleRemove={handleRemove}
                            analyzeContract={analyzeContract}
                        />
                    )}

                    {/* Markdown Output Component Container */}
                    <div className="max-w-none w-full animate-in fade-in slide-in-from-bottom-4 duration-500 pb-16">
                        <ContractAnalysisResult analysis={analysis} />
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Contracts;
