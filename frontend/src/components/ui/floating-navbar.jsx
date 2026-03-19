"use client";
import React, { useState } from "react";
import {
  motion,
  AnimatePresence,
  useScroll,
  useMotionValueEvent,
} from "framer-motion";

const MotionDiv = motion.div;
import { cn } from "../../lib/utils";
import ThemeToggle from "../ThemeToggle";
import { Button } from "./button";
import { useAuth } from "../../context/AuthContext";

export const FloatingNav = ({
  navItems,
  className
}) => {
  const { scrollYProgress } = useScroll();
  const [visible, setVisible] = useState(true);
  const [isTop, setIsTop] = useState(true);
  const { user } = useAuth();

  useMotionValueEvent(scrollYProgress, "change", (current) => {
    if (typeof current === "number") {
      const direction = current - scrollYProgress.getPrevious();
      const scrollY = window.scrollY;

      if (scrollY < 50) {
        setIsTop(true);
        setVisible(true);
      } else {
        setIsTop(false);
        if (direction < 0) {
          setVisible(true);
        } else {
          setVisible(false);
        }
      }
    }
  });

  return (
    <AnimatePresence mode="wait">
      <MotionDiv
        initial={{
          opacity: 1,
          y: -100,
        }}
        animate={{
          y: visible ? 0 : -100,
          opacity: visible ? 1 : 0,
        }}
        transition={{
          duration: 0.2,
        }}
        className={cn(
          "fixed inset-x-0 z-[5000] flex items-center justify-center transition-all duration-300 ease-in-out",
          isTop ? "top-0 w-full" : "top-10 max-w-fit mx-auto",
          className
        )}>
        <motion.div
           animate={{
            width: isTop ? "100%" : "auto",
            borderRadius: isTop ? "0px" : "9999px",
            borderTopWidth: isTop ? "0px" : "1px",
            borderLeftWidth: isTop ? "0px" : "1px",
            borderRightWidth: isTop ? "0px" : "1px",
            paddingTop: isTop ? "1rem" : "0.375rem",
            paddingBottom: isTop ? "1rem" : "0.375rem",
            paddingLeft: isTop ? "2rem" : "0.5rem",
            paddingRight: isTop ? "2rem" : "0.5rem",
          }}
          transition={{ duration: 0.3, ease: "easeInOut" }}
          className={cn(
            "flex items-center justify-between shadow-lg backdrop-blur-md transition-all duration-300 ease-in-out relative",
            isTop ? "border-b border-[#051326] bg-[#051326] text-white" : "border-[var(--card-border)] bg-[var(--background)]/80 text-[var(--foreground)]"
          )}>

          {/* Logo / Brand Name */}
          <div className={cn("flex flex-1 items-center gap-2 overflow-hidden shrink-0", isTop ? "pl-4 md:pl-8" : "hidden")}>
            <div className="w-8 h-8 rounded-full bg-[#d4af37] text-white flex items-center justify-center font-bold text-sm">
              AL
            </div>
            <span className={cn("font-bold text-lg whitespace-nowrap hidden md:block", isTop ? "text-white" : "text-[var(--foreground)]")}>AI-Lawyer</span>
          </div>

          {/* Nav items container */}
          <div className={cn("flex items-center justify-center transition-all", isTop ? "flex-none" : "flex-none")}>
            <div className="flex items-center gap-1">
            {navItems.map((navItem, idx) => (
              <a
                key={"link-"+idx}
                href={navItem.link}
                className={cn(
                  "relative flex items-center gap-1 rounded-full px-4 py-2 text-sm font-medium transition-colors hover:opacity-75",
                  isTop ? "text-white uppercase text-xs tracking-widest font-semibold" : "text-[var(--foreground)] hover:bg-foreground/5"
                )}>
                <span className="block sm:hidden">{navItem.icon}</span>
                <span className="hidden sm:block">{navItem.name}</span>
              </a>
            ))}
            </div>
          </div>

          {/* Right Section: Theme Toggle & CTA */}
          <div className={cn("flex flex-1 items-center justify-end gap-3", isTop ? "pr-4 md:pr-8" : "pl-2 pr-2 flex-none")}>
            <ThemeToggle />
            <Button asChild size="sm" className={cn("rounded-full font-semibold px-6 transition-colors", isTop ? "bg-[#d4af37] text-[#051326] hover:bg-[#c29e2f]" : "bg-[var(--foreground)] text-[var(--background)] hover:bg-[var(--foreground)]/90")}>
              {user ? (
                <a href="/dashboard">Dashboard</a>
              ) : (
                <a href="/auth?mode=login">Login</a>
              )}
            </Button>
          </div>
        </motion.div>
      </MotionDiv>
    </AnimatePresence>
  );
};
