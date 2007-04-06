/*
 * Copyright (c) 2005 Kinetic Networks, Inc. All Rights Reserved.
 */

package com.kni.etl.sessionizer;


/**
 * Insert the type's description here.
 * Creation date: (4/20/2002 4:35:34 PM)
 * @author: Administrator
 */
import com.kni.etl.EngineConstants;
import com.kni.etl.ketl_v1.DataItem;
import com.kni.etl.ketl_v1.ResultRecord;


public class PageView extends ResultRecord
{
    /**
     *
     */
    private static final long serialVersionUID = 3256438127357671479L;
    private long SessionID;
    public Session Session = null;
    transient public PageViewItemFinderAccelerator ItemFinderAccelerator = null;
    public static final int PAGEVIEW = 2;
    long mPageSequenceID = -1;

    /**
     * PageView constructor comment.
     */
    public PageView()
    {
        super();

        this.Type = PageView.PAGEVIEW;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getBrowser()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.BROWSER != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.BROWSER].ObjectType == EngineConstants.BROWSER)
            {
                return (this.LineFields[this.ItemFinderAccelerator.BROWSER]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.BROWSER);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.BROWSER = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getBytesSent()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.BYTES_SENT != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.BYTES_SENT].ObjectType == EngineConstants.BYTES_SENT)
            {
                return (this.LineFields[this.ItemFinderAccelerator.BYTES_SENT]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.BYTES_SENT);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.BYTES_SENT = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getCanonicalServerPort()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.CANONICAL_PORT != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.CANONICAL_PORT].ObjectType == EngineConstants.CANONICAL_PORT)
            {
                return (this.LineFields[this.ItemFinderAccelerator.CANONICAL_PORT]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.CANONICAL_PORT);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.CANONICAL_PORT = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:12:37 PM)
     * @return int
     * @param pItemID int
     */
    private int getFieldID(int pObjectType)
    {
        for (int i = 0; i < this.LineFields.length; i++)
        {
            if (this.LineFields[i].ObjectType == pObjectType)
            {
                return (i);
            }
        }

        return -1;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getGetRequest()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.GET_REQUEST != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.GET_REQUEST].ObjectType == EngineConstants.GET_REQUEST)
            {
                return (this.LineFields[this.ItemFinderAccelerator.GET_REQUEST]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.GET_REQUEST);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.GET_REQUEST = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getHitDateTime()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.HIT_DATE_TIME != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.HIT_DATE_TIME].ObjectType == EngineConstants.HIT_DATE_TIME)
            {
                return (this.LineFields[this.ItemFinderAccelerator.HIT_DATE_TIME]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.HIT_DATE_TIME);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.HIT_DATE_TIME = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getHTMLErrorCode()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.HTML_ERROR_CODE != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.HTML_ERROR_CODE].ObjectType == EngineConstants.HTML_ERROR_CODE)
            {
                return (this.LineFields[this.ItemFinderAccelerator.HTML_ERROR_CODE]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.HTML_ERROR_CODE);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.HTML_ERROR_CODE = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getInCookieString()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.IN_COOKIE != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.IN_COOKIE].ObjectType == EngineConstants.IN_COOKIE)
            {
                return (this.LineFields[this.ItemFinderAccelerator.IN_COOKIE]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.IN_COOKIE);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.IN_COOKIE = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getIPAddress()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.IP_ADDRESS != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.IP_ADDRESS].ObjectType == EngineConstants.IP_ADDRESS)
            {
                return (this.LineFields[this.ItemFinderAccelerator.IP_ADDRESS]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.IP_ADDRESS);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.IP_ADDRESS = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getOutCookieString()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.OUT_COOKIE != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.OUT_COOKIE].ObjectType == EngineConstants.OUT_COOKIE)
            {
                return (this.LineFields[this.ItemFinderAccelerator.OUT_COOKIE]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.OUT_COOKIE);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.OUT_COOKIE = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getReferrerURL()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.REFERRER_URL != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.REFERRER_URL].ObjectType == EngineConstants.REFERRER_URL)
            {
                return (this.LineFields[this.ItemFinderAccelerator.REFERRER_URL]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.REFERRER_URL);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.REFERRER_URL = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:09:44 PM)
     * @return java.lang.String
     */
    public String getRequestDirectory()
    {
        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:09:44 PM)
     * @return java.lang.String
     */
    public String getRequestFile()
    {
        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:09:44 PM)
     * @return java.lang.String
     */
    public DataItem getServerName()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.SERVER_NAME != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.SERVER_NAME].ObjectType == EngineConstants.SERVER_NAME)
            {
                return (this.LineFields[this.ItemFinderAccelerator.SERVER_NAME]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.SERVER_NAME);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.SERVER_NAME = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:09:44 PM)
     * @return java.lang.String
     */
    public String getRequestProtocol()
    {
        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:09:44 PM)
     * @return java.lang.String
     */
    public String getRequestQuery()
    {
        return null;
    }

    public long getPageSequence()
    {
        return this.mPageSequenceID;
    }

    public void setPageSequenceID(long pPageSeq)
    {
        this.mPageSequenceID = pPageSeq;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getTimeTakenToServeRequest()
    {
        if ((this.ItemFinderAccelerator != null) && (this.ItemFinderAccelerator.SERVE_TIME != -1))
        {
            if (this.LineFields[this.ItemFinderAccelerator.SERVE_TIME].ObjectType == EngineConstants.SERVE_TIME)
            {
                return (this.LineFields[this.ItemFinderAccelerator.SERVE_TIME]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.SERVE_TIME);

            if (itemID != -1)
            {
                if (this.ItemFinderAccelerator != null)
                {
                    this.ItemFinderAccelerator.SERVE_TIME = itemID;
                }

                return (this.LineFields[itemID]);
            }
        }

        return null;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 2:16:51 PM)
     */
    public void uRLClean()
    {
    }

    DataItem diSessionID = null;

    public void setSessionID(long sessionID)
    {
        this.SessionID = sessionID;
        this.diSessionID = new DataItem();
        this.diSessionID.setLong(sessionID);
    }

    public long getSessionID()
    {
        return this.SessionID;
    }

    public DataItem getSessionIDAsDataItem()
    {
        return this.diSessionID;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ResultRecord#CopyTo(com.kni.etl.ResultRecord)
     */
    @Override
    public ResultRecord CopyTo(ResultRecord newRecord)
    {
        PageView rec = (PageView) newRecord;
        rec.SessionID = this.SessionID;
        rec.Session = this.Session;
        rec.ItemFinderAccelerator = this.ItemFinderAccelerator;
        rec.mPageSequenceID = this.mPageSequenceID;
        rec.diSessionID = this.diSessionID;

        return super.CopyTo(newRecord);
    }
}
