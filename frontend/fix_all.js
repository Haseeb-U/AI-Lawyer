const fs = require('fs');

const contracts = `import React, { useState, useRef } from 'react';
import { IconFileText, IconUpload, IconX, IconAlertTriangle, IconLoader, IconCheck } from '@tabler/icons-react';
import { marked } from 'marked';
import { useContractAnalysis } from '../hooks/useContractAnalysis';

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

    const renderAnalysis = () => {
        if (!analysis) return null;
        const html = marked(analysis);

        return (
            <div className="bg-white border border-slate-200 rounded-3xl p-8 shadow-sm prose prose-slate max-w-none mt-8 prose-headings:text-slate-900 prose-a:text-blue-600">
                <h3 className="text-2xl font-bold flex items-center gap-2 mb-6 pb-4 border-b border-slate-100">
                    <IconCheck className="text-rose-500" />
                    AI Review & Feedback
                </h3>
                <div dangerouslySetInnerHTML={{ __html: html }} />
            </div>
        );
    };

    return (
        <div className="w-full p-8 lg:p-12 max-w-6xl mx-auto flex flex-col h-full overflow-y-auto">
            <div className="mb-8">
                <h1 className="text-3xl font-bold text-slate-900 mb-2">Contract Analysis</h1>
                <p className="text-slate-500">Upload your legal contracts to instantly identify risks, loopholes, and standard obligations.</p>
            </div>

            {error && (
                <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-xl flex items-start gap-3 text-red-700">
                    <IconAlertTriangle className="shrink-0 mt-0.5" size={20} />
                    <div>
                        <p className="font-semibold text-sm">Upload Error</p>
                        <p className="text-sm opacity-90">{error}</p>
                    </div>
                </div>
            )}

            {!file ? (
                <div 
                    className={`bg-white border-2 text-center transition-colors border-dashed rounded-3xl p-16 flex flex-col items-center justify-center ${dragActive ? 'border-rose-400 bg-rose-50/50' : 'border-slate-200'}`}
                    onDragEnter={handleDrag}
                    onDragLeave={handleDrag}
                    onDragOver={handleDrag}
                    onDrop={handleDrop}
                >
                    <input 
                        type="file" 
                        ref={fileInputRef} 
                        onChange={handleChange} 
                        className="hidden" 
                        accept=".pdf,.txt" 
                    />
                    <div className="h-16 w-16 bg-rose-50 text-rose-600 rounded-2xl flex items-center justify-center mb-6 pointer-events-none">
                        <IconUpload size={32} />
                    </div>
                    <h3 className="text-lg font-semibold text-slate-900 mb-2 pointer-events-none">Drag and drop your contract here</h3>
                    <p className="text-slate-500 text-sm mb-6 pointer-events-none">Supports PDF and TXT files up to 10MB.</p>
                    <button 
                        onClick={() => fileInputRef.current?.click()}
                        className="px-6 py-2.5 bg-slate-900 text-white rounded-lg hover:bg-slate-800 transition-colors font-medium z-10 relative"
                    >
                        Browse Files
                    </button>
                </div>
            ) : (
                <div className="bg-white border border-slate-200 rounded-3xl p-8 flex flex-col gap-6 shadow-sm">
                    <div className="flex items-center justify-between p-4 bg-slate-50 rounded-2xl border border-slate-100">
                        <div className="flex items-center gap-4">
                            <div className="h-12 w-12 bg-white text-rose-600 rounded-xl flex items-center justify-center shadow-sm border border-slate-100">
                                <IconFileText size={24} />
                            </div>
                            <div>
                                <p className="font-semibold text-slate-900 truncate max-w-md">{file.name}</p>
                                <p className="text-xs text-slate-500">{(file.size / 1024 / 1024).toFixed(2)} MB • {file.type.split('/')[1]?.toUpperCase() || 'TXT'}</p>
                            </div>
                        </div>
                        <button 
                            onClick={handleRemove}
                            disabled={loading}
                            className="p-2 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded-lg transition-colors disabled:opacity-50"
                        >
                            <IconX size={20} />
                        </button>
                    </div>

                    {!analysis && (
                        <div className="flex flex-col items-center pt-4">
                            {loading ? (
                                <div className="w-full space-y-4">
                                    <div className="flex justify-between text-sm text-slate-600 font-medium">
                                        <span>Analyzing document...</span>
                                        <span>{progress}%</span>
                                    </div>
                                    <div className="w-full bg-slate-100 rounded-full h-2.5 overflow-hidden">
                                        <div 
                                            className="bg-rose-500 h-2.5 rounded-full transition-all duration-300 ease-out"
                                            style={{ width: `${progress}%` }}
                                        ></div>
                                    </div>
                                    <div className="flex justify-center mt-4">
                                        <IconLoader className="animate-spin text-rose-500" size={32} />
                                    </div>
                                </div>
                            ) : (
                                <button 
                                    onClick={analyzeContract}
                                    className="px-8 py-4 w-full md:w-auto bg-rose-600 text-white rounded-2xl hover:bg-rose-700 transition-colors font-semibold text-lg shadow-sm shadow-rose-200"
                                >
                                    Start Full AI Analysis
                                </button>
                            )}
                        </div>
                    )}
                </div>
            )}

            {renderAnalysis()}
        </div>
    );
};

export default Contracts;
`;

const judgments = `import React, { useState } from 'react';
import { IconSearch, IconFileDescription, IconArrowRight, IconBook2, IconFilter, IconLoader, IconBalance, IconX } from '@tabler/icons-react';
import { useJudgments } from '../hooks/useJudgments';

const Judgments = () => {
    const {
        judgments,
        loading,
        error,
        search,
        setSearch,
        page,
        hasMore,
        summary,
        summaryLoading,
        summaryError,
        fetchJudgments,
        fetchSummary
    } = useJudgments();

    const [isHovered, setIsHovered] = useState(null);
    const [selectedJudgment, setSelectedJudgment] = useState(null);
    const [isSummaryModalOpen, setIsSummaryModalOpen] = useState(false);

    const handleSearch = (e) => {
        e.preventDefault();
        fetchJudgments(true);
    };

    const loadMore = () => {
        if (!loading && hasMore) {
            fetchJudgments(false);
        }
    };

    const handleSummarize = (judgment) => {
        setSelectedJudgment(judgment);
        setIsSummaryModalOpen(true);
        fetchSummary(judgment.id);
    };

    return (
        <div className="w-full p-8 lg:p-12 max-w-7xl mx-auto flex flex-col h-full overflow-y-auto">
            <div className="mb-10">
                <h1 className="text-4xl font-bold text-slate-900 mb-3">Judgments Library</h1>
                <p className="text-slate-500 text-lg">Access, search, and summarize historic court cases and verdicts instantly.</p>
            </div>

            <form onSubmit={handleSearch} className="relative z-20 w-full mb-12">
                <div className="relative flex items-center bg-white shadow-xl shadow-slate-200/50 rounded-full border border-slate-100 px-6 py-4 overflow-hidden focus-within:ring-2 focus-within:ring-rose-500 transition-shadow">
                    <IconSearch className="text-slate-400 mr-4" size={24} />
                    <input
                        type="text"
                        placeholder="Search by keyword, case title, judge name, or citation..."
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                        className="flex-1 bg-transparent outline-none text-lg text-slate-800 placeholder:text-slate-400 font-medium"
                    />
                    <button 
                        type="submit"
                        className="ml-4 bg-rose-600 text-white px-8 py-3 rounded-full font-bold tracking-wide hover:bg-rose-700 transition-colors shadow-md shadow-rose-200"
                        disabled={loading}
                    >
                        {loading && page === 1 ? <IconLoader className="animate-spin" /> : 'Search'}
                    </button>
                </div>
            </form>

            {error && (
                <div className="p-4 border-l-4 border-red-500 bg-red-50 text-red-700 font-medium rounded-lg mb-8">
                    {error}
                </div>
            )}

            <div className="flex flex-col gap-6">
                {judgments.map((judgment) => (
                    <div 
                        key={judgment.id}
                        className="bg-white border border-slate-200 rounded-3xl p-6 lg:p-8 hover:shadow-lg hover:-translate-y-1 transition-all duration-300 grid grid-cols-1 lg:grid-cols-4 gap-6"
                        onMouseEnter={() => setIsHovered(judgment.id)}
                        onMouseLeave={() => setIsHovered(null)}
                    >
                        <div className="lg:col-span-3">
                            <div className="flex items-center gap-3 mb-4">
                                <span className="px-3 py-1 bg-rose-50 text-rose-600 text-sm font-semibold rounded-full border border-rose-100">
                                    {judgment.court || 'Court'}
                                </span>
                                <span className="text-sm text-slate-500 font-medium">
                                    {judgment.date || 'Unknown Date'}
                                </span>
                            </div>
                            <h3 className="text-2xl font-bold text-slate-900 mb-2 leading-tight block hover:text-rose-600 transition-colors">
                                {judgment.title}
                            </h3>
                            <p className="text-slate-600 line-clamp-3 leading-relaxed mt-4">
                                {judgment.snippet || 'No snippet available for this judgment.'}
                            </p>
                        </div>
                        <div className="flex lg:flex-col justify-end lg:justify-between items-start gap-4 border-t lg:border-t-0 lg:border-l border-slate-100 pt-6 lg:pt-0 lg:pl-6">
                            <button 
                                onClick={() => handleSummarize(judgment)}
                                className="flex items-center justify-center gap-2 w-full px-4 py-3 bg-slate-50 hover:bg-rose-50 text-slate-700 hover:text-rose-600 font-semibold rounded-xl transition-colors border border-slate-200 hover:border-rose-200"
                            >
                                <IconFileDescription size={20} />
                                Summarize AI
                            </button>
                            <a 
                                href={judgment.url || '#'}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="flex items-center justify-center gap-2 w-full px-4 py-3 bg-slate-900 hover:bg-slate-800 text-white font-semibold rounded-xl transition-colors shadow-sm"
                            >
                                Full Verdict
                                <IconArrowRight size={20} />
                            </a>
                        </div>
                    </div>
                ))}
            </div>

            {judgments.length > 0 && hasMore && (
                <div className="mt-12 flex justify-center">
                    <button 
                        onClick={loadMore}
                        disabled={loading}
                        className="flex items-center gap-2 px-8 py-4 bg-white border-2 border-slate-200 text-slate-700 font-bold rounded-2xl hover:border-slate-300 hover:bg-slate-50 transition-colors"
                    >
                        {loading ? <IconLoader className="animate-spin" /> : 'Load More Judgments'}
                    </button>
                </div>
            )}

            {judgments.length === 0 && !loading && !error && (
                <div className="py-20 flex flex-col items-center justify-center text-center opacity-50">
                    <IconBalance size={64} className="text-slate-400 mb-4" />
                    <h3 className="text-xl font-bold text-slate-900">No judgments found</h3>
                    <p className="text-slate-500">Try adjusting your search criteria or keywords.</p>
                </div>
            )}

            {/* Summary Modal */}
            {isSummaryModalOpen && (
                <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/40 backdrop-blur-sm">
                    <div className="bg-white rounded-3xl w-full max-w-2xl max-h-[85vh] flex flex-col shadow-2xl overflow-hidden animate-in fade-in zoom-in duration-200">
                        <div className="p-6 border-b border-slate-100 flex justify-between items-center bg-slate-50">
                            <h2 className="text-xl font-bold text-slate-900 line-clamp-1 pr-4">
                                AI Summary: {selectedJudgment?.title}
                            </h2>
                            <button 
                                onClick={() => setIsSummaryModalOpen(false)}
                                className="text-slate-400 hover:text-red-500 transition-colors"
                            >
                                <IconX size={24} />
                            </button>
                        </div>
                        <div className="p-6 overflow-y-auto prose prose-slate max-w-none">
                            {summaryLoading ? (
                                <div className="flex flex-col items-center justify-center py-12 text-slate-500">
                                    <IconLoader className="animate-spin mb-4 text-rose-500" size={32} />
                                    <p className="font-medium">Generating intelligent summary...</p>
                                </div>
                            ) : summaryError ? (
                                <div className="p-4 bg-red-50 text-red-700 rounded-xl border border-red-100">
                                    {summaryError}
                                </div>
                            ) : (
                                <div className="leading-relaxed text-slate-700">
                                    {summary ? (
                                         <div dangerouslySetInnerHTML={{ __html: summary.replace(/\n/g, '<br />') }} />
                                    ) : (
                                        <p className="italic text-slate-400">Summary not available.</p>
                                    )}
                                </div>
                            )}
                        </div>
                        <div className="p-6 border-t border-slate-100 bg-slate-50 flex justify-end">
                            <button 
                                onClick={() => setIsSummaryModalOpen(false)}
                                className="px-6 py-2.5 bg-slate-900 text-white rounded-xl font-semibold hover:bg-slate-800 transition-colors"
                            >
                                Close Summary
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default Judgments;
`;

const search = `import React, { useRef, useState, useEffect } from 'react';
import { 
    IconSearch, IconFilter, IconArrowRight, IconBook2, IconLoader, 
    IconChevronDown, IconCheck, IconX, IconFileDescription
} from '@tabler/icons-react';
import { useSearch } from '../hooks/useSearch';

const Search = () => {
    const {
        searchQuery,
        setSearchQuery,
        activeTab,
        setActiveTab,
        selectedDocType,
        setSelectedDocType,
        documentTypes,
        results,
        loading,
        loadingTypes,
        error,
        page,
        hasMore,
        handleSearch,
        loadMore
    } = useSearch();

    const [isFilterOpen, setIsFilterOpen] = useState(false);
    const filterRef = useRef(null);

    // Close filter dropdown on outside click
    useEffect(() => {
        const handleClickOutside = (event) => {
            if (filterRef.current && !filterRef.current.contains(event.target)) {
                setIsFilterOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const onSubmit = (e) => {
        e.preventDefault();
        handleSearch(true);
    };

    return (
        <div className="w-full p-8 lg:p-12 max-w-7xl mx-auto flex flex-col h-full overflow-y-auto">
            <div className="mb-10 text-center lg:text-left">
                <h1 className="text-4xl font-extrabold text-slate-900 mb-4 tracking-tight">Legal Research Engine</h1>
                <p className="text-slate-500 text-lg max-w-2xl">Search across our comprehensive database of legal documents, templates, and precedents instantly.</p>
            </div>

            <div className="flex flex-col gap-6 mb-12 relative z-20">
                <form onSubmit={onSubmit} className="relative z-30 flex items-center bg-white shadow-xl shadow-slate-200/50 rounded-2xl border border-slate-100 p-2 overflow-visible focus-within:ring-2 focus-within:ring-rose-500 transition-all">
                    <div className="pl-4 text-slate-400">
                        <IconSearch size={24} />
                    </div>
                    <input
                        type="text"
                        placeholder="Search legal terms, clauses, or template names..."
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        className="flex-1 bg-transparent border-none outline-none text-lg text-slate-800 placeholder:text-slate-400 font-medium px-4 py-3 min-w-0"
                    />
                    
                    <div className="hidden md:flex gap-2 mx-4">
                        <div className="relative" ref={filterRef}>
                            <button
                                type="button"
                                onClick={() => setIsFilterOpen(!isFilterOpen)}
                                className="flex items-center gap-2 px-4 py-2.5 bg-slate-50 border border-slate-200 text-slate-700 font-medium rounded-xl hover:bg-slate-100 transition-colors"
                            >
                                <IconFilter size={18} />
                                {selectedDocType ? `Type: ${documentTypes.find(t => t.id === selectedDocType)?.name || selectedDocType}` : 'All Document Types'}
                                <IconChevronDown size={16} className={`transition-transform ${isFilterOpen ? 'rotate-180' : ''}`} />
                            </button>
                            
                            {isFilterOpen && (
                                <div className="absolute top-full mt-2 right-0 w-64 bg-white rounded-2xl shadow-xl border border-slate-100 py-2 z-50 animate-in fade-in slide-in-from-top-2">
                                    <button
                                        type="button"
                                        onClick={() => { setSelectedDocType(''); setIsFilterOpen(false); }}
                                        className={`w-full text-left px-4 py-2.5 flex items-center justify-between hover:bg-slate-50 transition-colors ${selectedDocType === '' ? 'text-rose-600 font-semibold bg-rose-50' : 'text-slate-700'}`}
                                    >
                                        All Types
                                        {selectedDocType === '' && <IconCheck size={18} />}
                                    </button>
                                    <div className="h-px bg-slate-100 my-1 mx-4" />
                                    {loadingTypes ? (
                                        <div className="px-4 py-3 text-sm text-slate-500 flex items-center gap-2">
                                            <IconLoader className="animate-spin" size={16} /> Loading types...
                                        </div>
                                    ) : (
                                        documentTypes.map(type => (
                                            <button
                                                key={type.id}
                                                type="button"
                                                onClick={() => { setSelectedDocType(type.id); setIsFilterOpen(false); }}
                                                className={`w-full text-left px-4 py-2.5 flex items-center justify-between hover:bg-slate-50 transition-colors ${selectedDocType === type.id ? 'text-rose-600 font-semibold bg-rose-50' : 'text-slate-700'}`}
                                            >
                                                {type.name}
                                                {selectedDocType === type.id && <IconCheck size={18} />}
                                            </button>
                                        ))
                                    )}
                                </div>
                            )}
                        </div>
                    </div>

                    <button 
                        type="submit"
                        disabled={loading && page === 1}
                        className="bg-rose-600 text-white px-8 py-3.5 rounded-xl font-bold tracking-wide hover:bg-rose-700 transition-colors shadow-md shadow-rose-200 flex items-center gap-2 disabled:bg-rose-400"
                    >
                        {loading && page === 1 ? <IconLoader className="animate-spin" size={20} /> : 'Search'}
                    </button>
                </form>

                <div className="flex gap-2 overflow-x-auto pb-2 scrollbar-hide -mx-8 px-8 lg:mx-0 lg:px-0">
                    {['all', 'contracts', 'clauses', 'templates'].map((tab) => (
                        <button
                            key={tab}
                            onClick={() => setActiveTab(tab)}
                            className={`px-6 py-2.5 rounded-full font-semibold whitespace-nowrap transition-all ${
                                activeTab === tab 
                                ? 'bg-slate-900 text-white shadow-md' 
                                : 'bg-white text-slate-600 border border-slate-200 hover:border-slate-300 hover:bg-slate-50'
                            }`}
                        >
                            {tab.charAt(0).toUpperCase() + tab.slice(1)}
                        </button>
                    ))}
                </div>
            </div>

            {error && (
                <div className="p-4 border-l-4 border-red-500 bg-red-50 text-red-700 font-medium rounded-lg mb-8 flex items-start gap-3">
                    <IconX className="shrink-0" />
                    {error}
                </div>
            )}

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
                {results.map((result, idx) => (
                    <div 
                        key={idx}
                        className="bg-white border text-left border-slate-200 hover:border-rose-200 rounded-3xl p-6 hover:shadow-xl hover:shadow-rose-100/50 transition-all duration-300 group flex flex-col h-full"
                    >
                        <div className="flex items-start justify-between mb-4">
                            <div className="h-12 w-12 bg-rose-50 text-rose-600 rounded-2xl flex items-center justify-center group-hover:scale-110 group-hover:bg-rose-600 group-hover:text-white transition-all duration-300 shadow-sm">
                                {result.type === 'contract' ? <IconFileDescription size={24} /> : <IconBook2 size={24} />}
                            </div>
                            <span className="px-3 py-1 bg-slate-50 text-slate-500 text-xs font-bold uppercase tracking-wider rounded-lg border border-slate-100">
                                {result.type || 'Document'}
                            </span>
                        </div>
                        <h3 className="text-xl font-bold text-slate-900 mb-3 group-hover:text-rose-600 transition-colors line-clamp-2">
                            {result.title}
                        </h3>
                        <p className="text-slate-500 text-sm mb-6 flex-grow line-clamp-3 leading-relaxed">
                            {result.description || result.snippet || 'No preview available. Click to view full document contents.'}
                        </p>
                        <div className="flex items-center justify-between pt-4 border-t border-slate-100 mt-auto">
                            <span className="text-xs font-semibold text-slate-400 flex items-center gap-1">
                                {(result.matchScore || idx * 10) + 50}% Match
                            </span>
                            <button className="flex items-center gap-1 text-sm font-bold text-rose-600 hover:text-rose-700 transition-colors">
                                View Details <IconArrowRight size={16} className="group-hover:translate-x-1 transition-transform" />
                            </button>
                        </div>
                    </div>
                ))}
            </div>

            {results.length > 0 && hasMore && (
                <div className="mt-12 flex justify-center">
                    <button 
                        onClick={loadMore}
                        disabled={loading}
                        className="flex items-center gap-2 px-8 py-4 bg-white border-2 border-slate-200 text-slate-700 font-bold rounded-2xl hover:border-slate-300 hover:bg-slate-50 transition-colors"
                    >
                        {loading ? <IconLoader className="animate-spin" /> : 'Load More Results'}
                    </button>
                </div>
            )}
            
            {results.length === 0 && !loading && !error && searchQuery && (
                <div className="py-24 flex flex-col items-center justify-center text-center max-w-md mx-auto">
                    <div className="h-24 w-24 bg-slate-50 rounded-full flex items-center justify-center mb-6 border-8 border-white shadow-sm">
                        <IconSearch size={32} className="text-slate-300" />
                    </div>
                    <h3 className="text-2xl font-bold text-slate-900 mb-2">No matches found</h3>
                    <p className="text-slate-500">We couldn't find any documents matching "{searchQuery}". Try adjusting your keywords or filters.</p>
                </div>
            )}
        </div>
    );
};

export default Search;
`;

const chatbot = `import React, { useRef, useEffect } from 'react';
import { IconSend, IconRobot, IconUser, IconLoader } from '@tabler/icons-react';
import { marked } from 'marked';
import { useChatbot } from '../hooks/useChatbot';

const Chatbot = () => {
    const {
        messages,
        input,
        setInput,
        loading,
        handleSend,
        user
    } = useChatbot();

    const messagesEndRef = useRef(null);

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages, loading]);

    const onKeyPress = (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            handleSend();
        }
    };

    return (
        <div className="flex flex-col h-full bg-slate-50/50 relative">
            <div className="bg-white border-b border-slate-200 px-8 py-6 flex-shrink-0 z-10 shadow-sm">
                <div className="flex items-center gap-4 max-w-5xl mx-auto">
                    <div className="h-12 w-12 bg-rose-100 text-rose-600 rounded-2xl flex items-center justify-center shadow-inner">
                        <IconRobot size={28} />
                    </div>
                    <div>
                        <h1 className="text-2xl font-bold text-slate-900 leading-tight">Legal AI Assistant</h1>
                        <p className="text-sm text-slate-500 font-medium">Ask questions about laws, contracts, or specific cases.</p>
                    </div>
                </div>
            </div>

            <div className="flex-1 overflow-y-auto w-full scroll-smooth p-4 hover:scrollbar-thumb-rose-200">
                <div className="max-w-4xl mx-auto space-y-6 pb-24">
                    
                    {messages.length === 0 && (
                        <div className="flex flex-col items-center justify-center py-20 text-center space-y-6 opacity-80">
                            <div className="h-24 w-24 bg-white shadow-xl rounded-full flex items-center justify-center border-4 border-slate-50 text-slate-300">
                                <IconRobot size={48} />
                            </div>
                            <div>
                                <h3 className="text-2xl font-semibold text-slate-900 tracking-tight">How can I help you today, {user?.name || 'User'}?</h3>
                                <p className="text-slate-500 mt-2 text-lg max-w-md mx-auto">I can analyze contracts, summarize court judgments, or answer legal queries.</p>
                            </div>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mt-8 w-full max-w-2xl">
                                {[
                                    'Summarize the latest supreme court ruling on contract breaches.',
                                    'What are the standard clauses in an NDA?',
                                    'Review my uploaded employment contract.',
                                    'Explain intellectual property rights under Pakistani law.'
                                ].map((suggestion, idx) => (
                                    <button 
                                        key={idx}
                                        onClick={() => { setInput(suggestion); }}
                                        className="text-left p-4 bg-white border border-slate-200 rounded-2xl hover:border-rose-300 hover:shadow-md transition-all text-sm font-medium text-slate-700 hover:text-rose-700"
                                    >
                                        {suggestion}
                                    </button>
                                ))}
                            </div>
                        </div>
                    )}

                    {messages.map((msg, index) => (
                        <div 
                            key={index} 
                            className={`flex ${msg.sender === 'user' ? 'justify-end' : 'justify-start'} w-full animate-in fade-in slide-in-from-bottom-2 duration-300`}
                        >
                            <div className={`flex gap-4 max-w-[85%] ${msg.sender === 'user' ? 'flex-row-reverse' : 'flex-row'}`}>
                                
                                <div className={`shrink-0 h-10 w-10 md:h-12 md:w-12 rounded-2xl flex items-center justify-center shadow-sm border
                                    ${msg.sender === 'user' 
                                        ? 'bg-gradient-to-br from-slate-800 to-slate-900 text-white border-slate-700' 
                                        : 'bg-gradient-to-br from-rose-500 to-rose-600 text-white border-rose-500'}`}
                                >
                                    {msg.sender === 'user' ? <IconUser size={24} /> : <IconRobot size={24} />}
                                </div>
                                
                                <div className={`flex flex-col ${msg.sender === 'user' ? 'items-end' : 'items-start'}`}>
                                    <div className={`px-6 py-4 rounded-3xl shadow-sm text-[15px] md:text-base leading-relaxed
                                        ${msg.sender === 'user' 
                                            ? 'bg-slate-900 text-white rounded-tr-sm' 
                                            : 'bg-white border text-left border-slate-200 text-slate-800 rounded-tl-sm prose prose-slate max-w-none prose-p:leading-relaxed prose-a:text-rose-600 hover:prose-a:text-rose-700'}`}
                                        dangerouslySetInnerHTML={{ 
                                            __html: msg.sender === 'ai' 
                                                ? marked(msg.text || '') 
                                                : msg.text 
                                        }}
                                    />
                                    <span className="text-[11px] text-slate-400 mt-2 px-2 font-medium">
                                        {new Date(msg.timestamp || Date.now()).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                                    </span>
                                </div>
                            </div>
                        </div>
                    ))}

                    {loading && (
                        <div className="flex justify-start w-full animate-in fade-in slide-in-from-bottom-2">
                            <div className="flex gap-4 max-w-[80%]">
                                <div className="shrink-0 h-12 w-12 bg-rose-100 text-rose-600 rounded-2xl flex items-center justify-center shadow-sm border border-rose-200">
                                    <IconRobot size={24} />
                                </div>
                                <div className="bg-white border border-slate-200 rounded-3xl rounded-tl-sm px-6 py-4 shadow-sm flex items-center gap-2">
                                    <span className="flex gap-1.5">
                                        <span className="w-2 h-2 bg-rose-400 rounded-full animate-bounce"></span>
                                        <span className="w-2 h-2 bg-rose-400 rounded-full animate-bounce" style={{ animationDelay: '150ms' }}></span>
                                        <span className="w-2 h-2 bg-rose-400 rounded-full animate-bounce" style={{ animationDelay: '300ms' }}></span>
                                    </span>
                                </div>
                            </div>
                        </div>
                    )}
                    <div ref={messagesEndRef} className="h-4" />
                </div>
            </div>

            <div className="absolute bottom-0 w-full bg-gradient-to-t from-slate-50 via-slate-50 to-transparent pt-10 pb-6 px-4">
                <div className="max-w-4xl mx-auto">
                    <div className="relative flex items-end bg-white border border-slate-300 shadow-xl shadow-slate-200/50 rounded-3xl overflow-hidden focus-within:ring-2 focus-within:ring-rose-500 focus-within:border-transparent transition-all">
                        <textarea
                            value={input}
                            onChange={(e) => setInput(e.target.value)}
                            onKeyDown={onKeyPress}
                            placeholder="Ask any legal question... (Press Shift+Enter for new line)"
                            className="flex-1 max-h-48 min-h-[64px] bg-transparent outline-none text-slate-800 placeholder:text-slate-400 p-5 resize-none font-medium text-base leading-relaxed"
                            rows={1}
                            disabled={loading}
                        />
                        <div className="p-3 flex items-center justify-center bg-white">
                            <button 
                                onClick={handleSend}
                                disabled={!input.trim() || loading}
                                className="h-12 w-12 bg-rose-600 text-white rounded-2xl flex items-center justify-center hover:bg-rose-700 transition-colors disabled:opacity-50 disabled:hover:bg-rose-600 shadow-md shadow-rose-200"
                            >
                                {loading ? <IconLoader className="animate-spin" size={24} /> : <IconSend size={24} stroke={2.5} className="ml-1" />}
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default Chatbot;
`;

fs.writeFileSync('src/pages/Contracts.jsx', contracts);
fs.writeFileSync('src/pages/Judgments.jsx', judgments);
fs.writeFileSync('src/pages/Search.jsx', search);
fs.writeFileSync('src/pages/Chatbot.jsx', chatbot);

console.log("Successfully rewrote component files!");
