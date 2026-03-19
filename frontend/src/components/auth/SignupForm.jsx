import React from 'react';
// eslint-disable-next-line no-unused-vars
import { motion, AnimatePresence } from 'framer-motion';
import { AuthInput } from './AuthInput';
import { Button } from '../ui/button';

export const SignupForm = ({
    isLogin,
    formData,
    agreed,
    setAgreed,
    handleInputChange,
    handleSignup,
    loading,
    toggleMode,
    success,
    error
}) => {
    return (
        <div className={`absolute right-0 top-0 w-full md:w-1/2 h-full flex-col justify-center px-12 lg:px-16 z-10 bg-[var(--card-bg)] ${!isLogin ? 'flex' : 'hidden md:flex'}`}>
            <div className="w-full max-w-[320px] mx-auto mt-16 md:mt-0">
                <h2 className="text-2xl font-bold text-[var(--foreground)] mb-8">Create your account</h2>

                <AnimatePresence>
                    {success && (
                        <motion.div initial={{opacity:0}} animate={{opacity:1}} exit={{opacity:0}} className="text-green-600 text-xs mb-4 bg-green-50 p-2 rounded">
                            {success}
                        </motion.div>
                    )}
                    {error && (
                        <motion.div initial={{opacity:0}} animate={{opacity:1}} exit={{opacity:0}} className="text-red-500 text-xs mb-4 bg-red-50 p-2 rounded">
                            {error}
                        </motion.div>
                    )}
                </AnimatePresence>

                <form onSubmit={handleSignup}>
                    <AuthInput label="Name" name="username" type="text" placeholder="Enter your name" value={formData.username} onChange={handleInputChange} required />
                    <AuthInput label="E-mail Address" name="email" type="email" placeholder="Enter your email" value={formData.email} onChange={handleInputChange} required />
                    <AuthInput label="Password" name="password" type="password" placeholder="Enter your password" value={formData.password} onChange={handleInputChange} required />
                    
                    <div className="flex items-center gap-2 mt-4 mb-8">
                        <input type="checkbox" id="tc" checked={agreed} onChange={(e) => setAgreed(e.target.checked)} className="w-4 h-4 rounded mt-1 bg-[var(--card-bg)] border-[var(--card-border)] text-[var(--brand-500)] focus:ring-[var(--brand-500)]" />
                        <label htmlFor="tc" className="text-[12px] text-[var(--text-soft)] font-medium cursor-pointer">
                            By Signing Up, I Agree with <span className="text-[var(--foreground)] hover:underline font-bold">Terms & Conditions</span>
                        </label>
                    </div>

                    <div className="flex gap-3">
                        <Button 
                            type="submit" 
                            disabled={loading} 
                            className="flex-1 bg-[var(--brand-500)] text-[#051326] dark:text-[#051326] hover:bg-[var(--brand-600)] shadow-md h-11 text-sm font-semibold rounded-full"
                        >
                            {loading ? 'Wait...' : 'Sign Up'}
                        </Button>
                        <Button 
                            type="button" 
                            onClick={toggleMode}
                            variant="outline"
                            className="flex-1 border-[var(--card-border)] text-[var(--foreground)] hover:bg-black/5 dark:hover:bg-white/10 h-11 text-sm font-semibold rounded-full"
                        >
                            Sign In
                        </Button>
                    </div>

                    <p className="text-center mt-5 text-[13px] text-[var(--text-soft)]">
                        Already have an account?{' '}
                        <button
                            type="button"
                            onClick={toggleMode}
                            className="text-[var(--brand-600)] dark:text-[var(--brand-400)] font-semibold hover:underline"
                        >
                            Sign in
                        </button>
                    </p>
                </form>
            </div>
        </div>
    );
};
