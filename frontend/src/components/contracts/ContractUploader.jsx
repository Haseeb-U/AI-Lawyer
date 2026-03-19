import React from 'react';
import { IconFileText, IconUpload } from '@tabler/icons-react';
import { Button } from '../ui/button';

export const ContractUploader = ({
    dragActive,
    handleDrag,
    handleDrop,
    handleChange,
    fileInputRef
}) => {
    return (
        <div
            className={`bg-[var(--card-bg)] border-2 text-center transition-colors border-dashed rounded-[24px] p-16 flex flex-col items-center justify-center ${dragActive ? 'border-[#d4af37] bg-[#d4af37]/5' : 'border-[var(--card-border)] hover:border-[#d4af37]/50'}`}
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
                accept=".txt"
            />
            <div className="h-16 w-16 bg-[#051326] dark:bg-white/10 text-white rounded-2xl flex items-center justify-center mb-6 pointer-events-none shadow-md">
                <IconUpload size={32} stroke={1.5} className="text-[#d4af37]" />
            </div>
            <h3 className="text-[17px] font-semibold text-[var(--foreground)] mb-2 pointer-events-none tracking-tight">Drag and drop your contract here</h3>
            <p className="text-[var(--text-soft)] text-[13px] mb-8 pointer-events-none font-medium">Supports TXT files up to 10MB.</p>
            <Button
                onClick={() => fileInputRef.current?.click()}
                className="px-8 z-10 relative shadow-md bg-[#051326] text-[#d4af37] hover:bg-[#051326]/90 border border-[#d4af37]/30"
            >
                Browse Files
            </Button>
        </div>
    );
};
