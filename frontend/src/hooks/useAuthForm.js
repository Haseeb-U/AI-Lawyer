import { useState, useEffect } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { toast } from 'sonner';
import { useAuth } from '../context/AuthContext';
import { ROUTES } from '../constants/routes.constants';
import { audioNotification } from '../services/audioNotification';

export const useAuthForm = () => {
    const [searchParams] = useSearchParams();
    const mode = searchParams.get('mode') || 'login';
    const isLogin = mode === 'login';
    
    const navigate = useNavigate();
    const { login, register, user } = useAuth();

    const [formData, setFormData] = useState({
        username: '',
        email: '',
        password: '',
    });
    const [agreed, setAgreed] = useState(false);
    const [error, setError] = useState(null);
    const [success, setSuccess] = useState(null);
    const [loading, setLoading] = useState(false);

    // Redirect if already logged in
    useEffect(() => {
        if (user) navigate(ROUTES.DASHBOARD);
    }, [user, navigate]);

    // Clear errors when mode changes
    useEffect(() => {
        setError(null);
    }, [mode]);

    const handleInputChange = (e) => {
        setFormData({ ...formData, [e.target.name]: e.target.value });
    };

    const toggleMode = () => {
        audioNotification.play('click');
        setError(null);
        setSuccess(null);
        navigate(`${ROUTES.AUTH}?mode=${isLogin ? 'signup' : 'login'}`, { replace: true });
    };

    const handleLogin = async (e) => {
        e.preventDefault();
        setError(null);
        setSuccess(null);
        setLoading(true);
        audioNotification.play('notification');
        try {
            const result = await login(formData.email, formData.password);
            if (result.success) {
                audioNotification.play('success');
                toast.success('Login successful!');
                navigate(ROUTES.DASHBOARD);
            } else {
                audioNotification.play('error');
                setError(result.error || 'Invalid credentials');
                toast.error(result.error || 'Invalid credentials');
            }
        } catch {
            audioNotification.play('error');
            setError('Something went wrong. Please try again later.');
            toast.error('Something went wrong. Please try again later.');
        } finally {
            setLoading(false);
        }
    };

    const handleSignup = async (e) => {
        e.preventDefault();
        if (!agreed) {
            audioNotification.play('error');
            setError('Please agree to the Terms & Conditions.');
            toast.warning('Please agree to the Terms & Conditions.');
            return;
        }
        setError(null);
        setSuccess(null);
        setLoading(true);
        audioNotification.play('notification');
        try {
            const result = await register(formData.username, formData.email, formData.password);
            if (result.success) {
                audioNotification.play('success');
                setSuccess('Account created! Please sign in.');
                toast.success('Account created! Please sign in.');
                navigate(`${ROUTES.AUTH}?mode=login`, { replace: true });
                setFormData({ username: '', email: '', password: '' });
                setAgreed(false);
            } else {
                audioNotification.play('error');
                setError(result.error || 'Registration failed');
                toast.error(result.error || 'Registration failed');
            }
        } catch {
            audioNotification.play('error');
            setError('Something went wrong. Please try again later.');
            toast.error('Something went wrong. Please try again later.');
        } finally {
            setLoading(false);
        }
    };

    return {
        isLogin,
        formData,
        agreed,
        setAgreed,
        error,
        success,
        loading,
        handleInputChange,
        toggleMode,
        handleLogin,
        handleSignup
    };
};
