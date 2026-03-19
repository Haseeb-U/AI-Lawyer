import React, { useState } from 'react';
import { Outlet } from 'react-router-dom';
import AppSidebar from '../components/layout/AppSidebar';

const DashboardLayout = () => {
    const [open, setOpen] = useState(false);

    return (
        <div className="flex flex-col md:flex-row h-[100dvh] w-full bg-[var(--navbar-bg)] font-sans overflow-hidden">
            {/* Nav Component */}
            <AppSidebar open={open} setOpen={setOpen} />

            {/* Main Content Area populated by Router Outlet */}
            <main className="flex-1 flex flex-col bg-[var(--background)] overflow-hidden relative rounded-tl-3xl rounded-tr-3xl md:rounded-tr-none md:rounded-bl-3xl mt-0 md:mt-2 md:mr-2 md:mb-2 shadow-xl shrink-0 min-w-0 transition-all duration-300">
                <Outlet />
            </main>
        </div>
    );
};

export default DashboardLayout;
