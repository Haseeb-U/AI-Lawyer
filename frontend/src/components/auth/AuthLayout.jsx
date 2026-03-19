import React from 'react';

export const AuthLayout = ({ children, rightPanel }) => {
  return (
    <div className="min-h-screen w-full bg-[radial-gradient(circle_at_12%_10%,_#102a52_0%,_#0a1b37_34%,_#e8ecf1_100%)] flex items-center justify-center p-4 sm:p-6 lg:p-8 font-sans">
      <div className="w-full max-w-[1200px] h-[85vh] min-h-[650px] bg-[#F7F6F2] rounded-[2.5rem] shadow-[0_30px_90px_rgba(5,19,38,0.28)] border border-white/60 overflow-hidden flex relative">
        {/* Background ambient gradient for left side */}
        <div className="absolute bottom-0 left-0 w-1/2 h-2/3 bg-gradient-to-tr from-[#d4af37]/20 via-transparent to-transparent pointer-events-none"></div>

        {/* Left Column - Form */}
        <div className="w-full md:w-[45%] lg:w-[45%] h-full relative z-10 flex flex-col p-8 sm:p-10 lg:p-12 overflow-y-auto">
          {children}
        </div>

        {/* Right Column - Graphic */}
        <div className="hidden md:block md:w-[55%] lg:w-[55%] h-full p-4 pl-0 relative z-10">
          {rightPanel}
        </div>
      </div>
    </div>
  );
};
