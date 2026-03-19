"use client";
import { cn } from "@/lib/utils";
import React, { useState, createContext, useContext } from "react";
import { AnimatePresence } from "motion/react";
import { motion } from "framer-motion";
import { IconMenu2, IconX } from "@tabler/icons-react";
import { Link } from "react-router-dom";

const SidebarContext = createContext(undefined);

// eslint-disable-next-line react-refresh/only-export-components
export const useSidebar = () => {
  const context = useContext(SidebarContext);
  if (!context) {
    throw new Error("useSidebar must be used within a SidebarProvider");
  }
  return context;
};

export const SidebarProvider = ({
  children,
  open: openProp,
  setOpen: setOpenProp,
  animate = true
}) => {
  const [openState, setOpenState] = useState(false);

  const open = openProp !== undefined ? openProp : openState;
  const setOpen = setOpenProp !== undefined ? setOpenProp : setOpenState;

  return (
    <SidebarContext.Provider value={{ open, setOpen, animate: animate }}>
      {children}
    </SidebarContext.Provider>
  );
};

export const Sidebar = ({
  children,
  open,
  setOpen,
  animate
}) => {
  return (
    <SidebarProvider open={open} setOpen={setOpen} animate={animate}>
      {children}
    </SidebarProvider>
  );
};

export const SidebarBody = (props) => {
  return (
    <>
      <DesktopSidebar {...props} />
      <MobileSidebar {...(props)} />
    </>
  );
};

export const DesktopSidebar = ({
  className,
  children,
  ...props
}) => {
  const { open, setOpen, animate } = useSidebar();
  return (
    <>
      <motion.div
        className={cn(
          "h-full px-3 py-4 hidden md:flex md:flex-col w-[200px] shrink-0",
          className
        )}
        animate={{
          width: animate ? (open ? "200px" : "64px") : "200px",
        }}
        onMouseEnter={() => setOpen(true)}
        onMouseLeave={() => setOpen(false)}
        {...props}>
        {children}
      </motion.div>
    </>
  );
};

export const MobileSidebar = ({
  className,
  children,
  ...props
}) => {
  const { open, setOpen } = useSidebar();
  return (
    <>
      <div
        className={cn(
          "h-[4.5rem] px-4 py-4 flex flex-row md:hidden items-center justify-start bg-[var(--navbar-bg)] border-b border-[#d4af37]/20 w-full gap-3 transition-all"
        )}
        {...props}>
        <IconMenu2
          className="text-white hover:text-[#d4af37] transition-colors cursor-pointer"
          size={28}
          onClick={() => setOpen(!open)} />
        <span className="text-white font-bold text-lg tracking-wide">AI-Lawyer</span>
        <AnimatePresence>
          {open && (
            <>
              {/* Opacity backdrop to close the drawer */}
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                className="fixed inset-0 bg-black/60 backdrop-blur-sm z-[90]"
                onClick={() => setOpen(false)}
              />
              {/* Drawer itself */}
              <motion.div
                initial={{ x: "-100%", opacity: 0 }}
                animate={{ x: 0, opacity: 1 }}
                exit={{ x: "-100%", opacity: 0 }}
                transition={{
                  duration: 0.3,
                  ease: "easeInOut",
                }}
                className={cn(
                  "fixed h-full w-[280px] max-w-[80vw] left-0 top-0 shadow-[4px_0_24px_rgba(0,0,0,0.5)] bg-[#051326] border-r border-[#d4af37]/20 p-5 z-[100] flex flex-col justify-between break-words",
                  className
                )}>
                <div
                  className="absolute right-4 top-5 p-2 z-50 rounded-full text-white/70 hover:text-white hover:bg-white/10 cursor-pointer transition-colors"
                  onClick={() => setOpen(!open)}>
                  <IconX size={20} />
                </div>
                {children}
              </motion.div>
            </>
          )}
        </AnimatePresence>
      </div>
    </>
  );
};

export const SidebarLink = ({ link, className, ...props }) => { const { open, animate, setOpen } = useSidebar(); return ( <Link to={link.href} onClick={() => setOpen(false)} className={cn("flex items-center justify-start gap-2 group/sidebar py-2", className)} {...props}> {link.icon} <motion.span animate={{ display: animate ? (open ? "inline-block" : "none") : "inline-block", opacity: animate ? (open ? 1 : 0) : 1, }} className="text-inherit text-sm group-hover/sidebar:translate-x-1 transition duration-150 whitespace-pre inline-block !p-0 !m-0"> {link.label} </motion.span> </Link> ); };
