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

        this.Type = PAGEVIEW;
    }

    /**
     * Insert the method's description here.
     * Creation date: (5/10/2002 12:06:48 PM)
     * @return java.lang.String
     */
    public DataItem getBrowser()
    {
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.BROWSER != -1))
        {
            if (LineFields[ItemFinderAccelerator.BROWSER].ObjectType == EngineConstants.BROWSER)
            {
                return (LineFields[ItemFinderAccelerator.BROWSER]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.BROWSER);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.BROWSER = itemID;
                }

                return (LineFields[itemID]);
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.BYTES_SENT != -1))
        {
            if (LineFields[ItemFinderAccelerator.BYTES_SENT].ObjectType == EngineConstants.BYTES_SENT)
            {
                return (LineFields[ItemFinderAccelerator.BYTES_SENT]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.BYTES_SENT);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.BYTES_SENT = itemID;
                }

                return (LineFields[itemID]);
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.CANONICAL_PORT != -1))
        {
            if (LineFields[ItemFinderAccelerator.CANONICAL_PORT].ObjectType == EngineConstants.CANONICAL_PORT)
            {
                return (LineFields[ItemFinderAccelerator.CANONICAL_PORT]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.CANONICAL_PORT);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.CANONICAL_PORT = itemID;
                }

                return (LineFields[itemID]);
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
        for (int i = 0; i < LineFields.length; i++)
        {
            if (LineFields[i].ObjectType == pObjectType)
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.GET_REQUEST != -1))
        {
            if (LineFields[ItemFinderAccelerator.GET_REQUEST].ObjectType == EngineConstants.GET_REQUEST)
            {
                return (LineFields[ItemFinderAccelerator.GET_REQUEST]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.GET_REQUEST);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.GET_REQUEST = itemID;
                }

                return (LineFields[itemID]);
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.HIT_DATE_TIME != -1))
        {
            if (LineFields[ItemFinderAccelerator.HIT_DATE_TIME].ObjectType == EngineConstants.HIT_DATE_TIME)
            {
                return (LineFields[ItemFinderAccelerator.HIT_DATE_TIME]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.HIT_DATE_TIME);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.HIT_DATE_TIME = itemID;
                }

                return (LineFields[itemID]);
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.HTML_ERROR_CODE != -1))
        {
            if (LineFields[ItemFinderAccelerator.HTML_ERROR_CODE].ObjectType == EngineConstants.HTML_ERROR_CODE)
            {
                return (LineFields[ItemFinderAccelerator.HTML_ERROR_CODE]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.HTML_ERROR_CODE);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.HTML_ERROR_CODE = itemID;
                }

                return (LineFields[itemID]);
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.IN_COOKIE != -1))
        {
            if (LineFields[ItemFinderAccelerator.IN_COOKIE].ObjectType == EngineConstants.IN_COOKIE)
            {
                return (LineFields[ItemFinderAccelerator.IN_COOKIE]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.IN_COOKIE);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.IN_COOKIE = itemID;
                }

                return (LineFields[itemID]);
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.IP_ADDRESS != -1))
        {
            if (LineFields[ItemFinderAccelerator.IP_ADDRESS].ObjectType == EngineConstants.IP_ADDRESS)
            {
                return (LineFields[ItemFinderAccelerator.IP_ADDRESS]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.IP_ADDRESS);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.IP_ADDRESS = itemID;
                }

                return (LineFields[itemID]);
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.OUT_COOKIE != -1))
        {
            if (LineFields[ItemFinderAccelerator.OUT_COOKIE].ObjectType == EngineConstants.OUT_COOKIE)
            {
                return (LineFields[ItemFinderAccelerator.OUT_COOKIE]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.OUT_COOKIE);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.OUT_COOKIE = itemID;
                }

                return (LineFields[itemID]);
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.REFERRER_URL != -1))
        {
            if (LineFields[ItemFinderAccelerator.REFERRER_URL].ObjectType == EngineConstants.REFERRER_URL)
            {
                return (LineFields[ItemFinderAccelerator.REFERRER_URL]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.REFERRER_URL);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.REFERRER_URL = itemID;
                }

                return (LineFields[itemID]);
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.SERVER_NAME != -1))
        {
            if (LineFields[ItemFinderAccelerator.SERVER_NAME].ObjectType == EngineConstants.SERVER_NAME)
            {
                return (LineFields[ItemFinderAccelerator.SERVER_NAME]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.SERVER_NAME);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.SERVER_NAME = itemID;
                }

                return (LineFields[itemID]);
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
        return mPageSequenceID;
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
        if ((ItemFinderAccelerator != null) && (ItemFinderAccelerator.SERVE_TIME != -1))
        {
            if (LineFields[ItemFinderAccelerator.SERVE_TIME].ObjectType == EngineConstants.SERVE_TIME)
            {
                return (LineFields[ItemFinderAccelerator.SERVE_TIME]);
            }
        }
        else
        {
            int itemID = this.getFieldID(EngineConstants.SERVE_TIME);

            if (itemID != -1)
            {
                if (ItemFinderAccelerator != null)
                {
                    ItemFinderAccelerator.SERVE_TIME = itemID;
                }

                return (LineFields[itemID]);
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
        SessionID = sessionID;
        diSessionID = new DataItem();
        diSessionID.setLong(sessionID);
    }

    public long getSessionID()
    {
        return SessionID;
    }

    public DataItem getSessionIDAsDataItem()
    {
        return diSessionID;
    }

    /* (non-Javadoc)
     * @see com.kni.etl.ResultRecord#CopyTo(com.kni.etl.ResultRecord)
     */
    public ResultRecord CopyTo(ResultRecord newRecord)
    {
        PageView rec = (PageView) newRecord;
        rec.SessionID = SessionID;
        rec.Session = Session;
        rec.ItemFinderAccelerator = ItemFinderAccelerator;
        rec.mPageSequenceID = mPageSequenceID;
        rec.diSessionID = diSessionID;

        return super.CopyTo(newRecord);
    }
}
