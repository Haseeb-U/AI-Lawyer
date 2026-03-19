import React from 'react';
// eslint-disable-next-line no-unused-vars
import { motion } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import { BrandMark } from './BrandMark';

export const AuthRightPanel = () => {
  const navigate = useNavigate();

  return (
    <div className="w-full h-full bg-[#051326] rounded-[2.1rem] overflow-hidden relative flex flex-col items-center justify-center">
        {/* Close/Back button inside the graphic panel as seen in reference */}
        <button 
           onClick={() => navigate('/')} 
           className="absolute top-6 right-6 w-10 h-10 bg-white/10 hover:bg-white/20 transition-colors rounded-full flex items-center justify-center cursor-pointer z-50 text-white backdrop-blur-md border border-white/10 shadow-sm"
        >
           <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><path d="M18 6L6 18M6 6l12 12"/></svg>
        </button>

        {/* Ambient Glow matching our brand */}
        <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[150%] h-[150%] bg-[radial-gradient(ellipse_at_center,_var(--tw-gradient-stops))] from-[#d4af37]/20 via-[#051326]/5 to-[#051326] pointer-events-none z-0"></div>

        {/* Central Graphic (Scales of Justice Abstract Icon) */}
        <motion.div 
           initial={{ scale: 0.9, opacity: 0 }}
           animate={{ scale: 1, opacity: 1 }}
           transition={{ duration: 0.8, ease: 'easeOut' }}
           className="relative z-10"
        >
            <div className="w-52 h-52 rounded-full border border-[#d4af37]/25 flex items-center justify-center relative bg-gradient-to-b from-white/10 to-white/[0.04] backdrop-blur-md shadow-2xl">
              <BrandMark size={120} stroke="#f3db96" accent="#f8e3a9" strokeWidth={2.7} mode="mono" />
              <div className="absolute inset-5 rounded-full border border-[#d4af37]/20"></div>
            </div>
            
            {/* Pulsing ring */}
            <div className="absolute inset-0 rounded-full border border-[#d4af37]/10 animate-ping"></div>
        </motion.div>

        {/* Floating Tag 1 (Simulating the UI Card in the reference) */}
        <motion.div
           animate={{ y: [0, -15, 0] }}
           transition={{ duration: 4, repeat: Infinity, ease: "easeInOut" }}
           className="absolute top-[20%] left-8 bg-[#ffffff]/10 backdrop-blur-md rounded-2xl p-4 border border-[#ffffff]/10 shadow-[0_8px_32px_rgba(0,0,0,0.3)] w-56 z-20"
        >
           <div className="flex items-center gap-3 mb-2">
             <div className="w-7 h-7 rounded-md bg-[#d4af37] flex items-center justify-center text-[#051326]">
               <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5"><path d="M5 12l5 5l10 -10"/></svg>
             </div>
             <div className="text-white text-[13px] font-medium tracking-wide">Contract Review</div>
           </div>
           <div className="text-white/60 text-[11px] ml-[40px] pl-1 font-medium">Processing 3 active drafts</div>
        </motion.div>

        {/* Floating Tag 2 (Simulating the Calender/Meeting Card in reference) */}
        <motion.div
           animate={{ y: [0, 10, 0] }}
           transition={{ duration: 5, repeat: Infinity, ease: "easeInOut", delay: 1 }}
           className="absolute bottom-[20%] right-8 bg-[#ffffff]/10 backdrop-blur-md rounded-2xl p-4 border border-[#ffffff]/10 shadow-[0_8px_32px_rgba(0,0,0,0.3)] w-60 z-20"
        >
           <div className="text-white text-[13px] font-medium mb-3 tracking-wide">Case Hearing Priority</div>
           <div className="bg-[#ffffff]/5 rounded-xl p-2.5 flex justify-between items-center">
             <div className="flex -space-x-2">
               <div className="w-6 h-6 rounded-full border border-[#051326] bg-[#d4af37] flex items-center justify-center text-[#051326] text-[10px] font-bold">JD</div>
               <div className="w-6 h-6 rounded-full border border-[#051326] bg-gray-300 flex items-center justify-center text-[#051326] text-[10px] font-bold">SK</div>
               <div className="w-6 h-6 rounded-full border border-[#051326] bg-[#ffffff] flex items-center justify-center text-[10px] text-black font-semibold">+2</div>
             </div>
             <span className="text-white text-[11px] font-medium">Tomorrow, 10:00 AM</span>
           </div>
        </motion.div>
    </div>
  );
};
